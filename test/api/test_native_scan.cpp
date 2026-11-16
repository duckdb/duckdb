#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/progress_data.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "test_helpers.hpp"

#ifndef DUCKDB_NO_THREADS
#include <chrono>
#include <condition_variable>
#include <future>
#endif

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS
namespace {
class NativeGatedFileSystem : public LocalFileSystem {
public:
	explicit NativeGatedFileSystem(string path) : path(std::move(path)) {
	}

	string GetName() const override {
		return "NativeGatedFileSystem";
	}

	bool CanHandleFile(const string &file) override {
		return file == path;
	}

	bool OnDiskFile(FileHandle &) override {
		return false;
	}

	bool IsLocalFileSystem() const override {
		return false;
	}

	void Read(FileHandle &handle, void *buffer, int64_t bytes, idx_t offset) override {
		{
			std::unique_lock<std::mutex> guard(lock);
			if (armed) {
				entered = true;
				changed.notify_all();
				if (!changed.wait_for(guard, std::chrono::seconds(25), [&]() { return released; })) {
					throw IOException("Test did not release the native scan read");
				}
			}
		}
		LocalFileSystem::Read(handle, buffer, bytes, offset);
	}

	void Arm() {
		std::lock_guard<std::mutex> guard(lock);
		armed = true;
	}

	bool WaitForRead() {
		std::unique_lock<std::mutex> guard(lock);
		return changed.wait_for(guard, std::chrono::seconds(10), [&]() { return entered; });
	}

	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		released = true;
		changed.notify_all();
	}

private:
	const string path;
	std::mutex lock;
	std::condition_variable changed;
	bool armed = false;
	bool entered = false;
	bool released = false;
};

struct NativeReadRelease {
	NativeGatedFileSystem &gate;
	~NativeReadRelease() {
		gate.Release();
	}
};
} // namespace

TEST_CASE("Native scan affinity returns cached rows while remote reads are pending", "[api][storage]") {
	auto path = TestCreatePath("native_scan_remote.db");
	DeleteDatabase(path);
	const idx_t group_size = 32 * STANDARD_VECTOR_SIZE;
	const idx_t cold_rows = 4 * group_size;
	const idx_t expected_rows = 6 * group_size;
	{
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + StringUtil::Replace(path, "'", "''") + "' AS fixture (ROW_GROUP_SIZE " +
		                          to_string(group_size) + ")"));
		REQUIRE_NO_FAIL(con.Query("USE fixture"));
		REQUIRE_NO_FAIL(con.Query("SET force_compression='uncompressed'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i FROM range(" + to_string(cold_rows) + ") r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		// Separate checkpoints prevent cold and cached groups from sharing partial blocks.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i FROM range(" + to_string(cold_rows) + "," +
		                          to_string(expected_rows) + ") r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto groups = con.Query("SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('t')");
		REQUIRE_NO_FAIL(*groups);
		REQUIRE(CHECK_COLUMN(groups, 0, {6}));
		auto overlap =
		    con.Query("SELECT count(*) FROM (SELECT block_id FROM pragma_storage_info('t') "
		              "WHERE column_name='i' AND segment_type='BIGINT' AND row_group_id<4 AND block_id>=0 "
		              "INTERSECT SELECT block_id FROM pragma_storage_info('t') "
		              "WHERE column_name='i' AND segment_type='BIGINT' AND row_group_id>=4 AND block_id>=0)");
		REQUIRE_NO_FAIL(*overlap);
		REQUIRE(CHECK_COLUMN(overlap, 0, {0}));
	}
	DuckDB db(nullptr);
	Connection con(db);
	// Native remote-only prefetch uses the file handle's classification, independently of its URL.
	auto &fs = FileSystem::GetFileSystem(*con.context);
	path = fs.CanonicalizePath(path);
	auto gated_fs = make_uniq<NativeGatedFileSystem>(path);
	auto &gate = *gated_fs;
	fs.RegisterSubSystem(std::move(gated_fs));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + StringUtil::Replace(path, "'", "''") + "' AS remote_scan (READ_ONLY)"));
	auto attached = DatabaseManager::Get(*con.context).GetDatabase("remote_scan");
	REQUIRE(attached);
	auto &blocks = attached->GetStorageManager().GetBlockManager();
	REQUIRE(blocks.IsRemote());
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='512MB'"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	// Let the first cached vector satisfy Fetch, including in small-vector builds.
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='1B'"));
	// All 192 vector assignments fit; four ordered consumers would block on the cold prefix.
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=256"));
	REQUIRE_NO_FAIL(con.Query("SET max_execution_time=20000"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='remote_only'"));
	REQUIRE(blocks.Prefetch());
	REQUIRE_NO_FAIL(con.Query("PRAGMA verify_parallelism"));
	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM pragma_storage_info('remote_scan.main.t')"));
	REQUIRE_NO_FAIL(con.Query("SELECT sum(i) FROM remote_scan.t WHERE i>=" + to_string(cold_rows)));
	REQUIRE_NO_FAIL(con.Query("CALL enable_logging('AsyncTaskSchedule')"));
	duckdb::unique_ptr<QueryResultStream<>> stream;
	std::future<duckdb::unique_ptr<DataChunk>> first;
	// Release before the future joins, including when setup or an assertion throws.
	NativeReadRelease release_on_exit {gate};
	gate.Arm();
	stream = OpenStream(con, "SELECT i FROM remote_scan.t");
	first = std::async(std::launch::async, [&]() { return stream->Fetch(); });
	const bool read_pending = gate.WaitForRead();
	const bool cache_ready = first.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
	gate.Release();
	auto chunk = first.get();
	REQUIRE(read_pending);
	REQUIRE(cache_ready);
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);
	for (idx_t row = 0; row < chunk->size(); row++) {
		REQUIRE(chunk->GetValue(0, row).GetValue<int64_t>() >= int64_t(cold_rows));
	}

	vector<bool> seen(expected_rows, false);
	idx_t rows = 0;
	bool valid = true;
	do {
		for (idx_t row = 0; row < chunk->size(); row++) {
			const auto i = chunk->GetValue(0, row).GetValue<int64_t>();
			if (i < 0 || idx_t(i) >= expected_rows) {
				valid = false;
				continue;
			}
			valid &= !seen[i];
			seen[i] = true;
			rows++;
		}
		chunk = stream->Fetch();
	} while (chunk);
	REQUIRE_FALSE(stream->HasError());
	REQUIRE(valid);
	REQUIRE(rows == expected_rows);
	stream.reset();
	auto scheduled =
	    con.Query("SELECT count(*)>0 FROM duckdb_logs_parsed('AsyncTaskSchedule') WHERE pool='READ_AHEAD'");
	REQUIRE_NO_FAIL(*scheduled);
	REQUIRE(CHECK_COLUMN(scheduled, 0, {true}));
}

TEST_CASE("Native scan affinity resumes asynchronous reads and releases abandoned work", "[api][storage]") {
	auto path = TestCreatePath("native_scan_async.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i, i%97 k FROM range(1228800) r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	// Reopen so the scan has to schedule reads for missing blocks.
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=0"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='debug_force_always'"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA verify_parallelism"));
	REQUIRE_NO_FAIL(con.Query("CALL enable_logging('AsyncTaskSchedule')"));

	auto stream = OpenStream(con, "SELECT i,k FROM t WHERE k>=0");
	REQUIRE_FALSE(stream->HasError());
	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);
	SECTION("Drain") {
		idx_t rows = 0;
		int64_t sum = 0;
		bool valid = true;
		do {
			for (idx_t row = 0; row < chunk->size(); row++) {
				const auto i = chunk->GetValue(0, row).GetValue<int64_t>();
				valid &= chunk->GetValue(1, row).GetValue<int64_t>() == i % 97;
				sum += i;
				rows++;
			}
			chunk = stream->Fetch();
		} while (chunk);
		REQUIRE_FALSE(stream->HasError());
		REQUIRE(valid);
		REQUIRE(rows == 1228800);
		REQUIRE(sum == int64_t(754974105600));
	}
	SECTION("Abandon") {
		// Drop a scan that still owns queued assignments, then run another query on the connection.
	}
	stream.reset();
	auto scheduled =
	    con.Query("SELECT count(*) FILTER (WHERE pool='ASYNC') > 0, "
	              "count(*) FILTER (WHERE pool='READ_AHEAD') FROM duckdb_logs_parsed('AsyncTaskSchedule')");
	REQUIRE_NO_FAIL(*scheduled);
	REQUIRE(CHECK_COLUMN(scheduled, 0, {true}));
	REQUIRE(CHECK_COLUMN(scheduled, 1, {0}));
	auto result = con.Query("SELECT count(*),sum(i)::BIGINT FROM t WHERE k>=0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1228800}));
	REQUIRE(CHECK_COLUMN(result, 1, {int64_t(754974105600)}));
}
#endif

#ifndef DUCKDB_NO_THREADS
TEST_CASE("Native scan read-ahead combines cached and missing assignments", "[api][storage]") {
	auto path = TestCreatePath("native_scan_mixed.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET force_compression='uncompressed'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i, i%97 k FROM range(1228800) r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=8"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='debug_force_always'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA verify_parallelism"));
	SECTION("Cold") {
	}
	SECTION("Mixed cache") {
		REQUIRE_NO_FAIL(con.Query("SELECT sum(i),sum(k) FROM t WHERE i<614400"));
	}
	SECTION("Low memory") {
		REQUIRE_NO_FAIL(con.Query("SET memory_limit='8MB'"));
		REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=-1"));
	}
	SECTION("Ordered positional children") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE u AS SELECT i+1 j FROM range(1228800) r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto result = con.Query("SELECT count(*) FROM t POSITIONAL JOIN u WHERE i+1<>j");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		return;
	}
	REQUIRE_NO_FAIL(con.Query("CALL enable_logging('AsyncTaskSchedule')"));
	auto result = con.Query("SELECT count(*),sum(i)::BIGINT FROM t WHERE k>=0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1228800}));
	REQUIRE(CHECK_COLUMN(result, 1, {int64_t(754974105600)}));
	auto scheduled =
	    con.Query("SELECT count(*)>0 FROM duckdb_logs_parsed('AsyncTaskSchedule') WHERE pool='READ_AHEAD'");
	REQUIRE_NO_FAIL(*scheduled);
	REQUIRE(CHECK_COLUMN(scheduled, 0, {true}));
}
#endif

TEST_CASE("Native scan assignments retain a streaming snapshot across checkpoints", "[api][storage]") {
	auto path = TestCreatePath("native_scan_snapshot.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection reader(db), writer(db);

#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(reader.Query("SET threads=4"));
#endif
	REQUIRE_NO_FAIL(reader.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(reader.Query("SET streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(reader.Query("SET storage_block_prefetch='never'"));
	REQUIRE_NO_FAIL(reader.Query("PRAGMA verify_parallelism"));
	REQUIRE_NO_FAIL(writer.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(writer.Query("CREATE TABLE t AS SELECT i, i%97 k FROM range(1228800) r(i)"));
	REQUIRE_NO_FAIL(writer.Query("CHECKPOINT"));
	REQUIRE_NO_FAIL(reader.Query("BEGIN TRANSACTION READ ONLY"));

	auto stream = OpenStream(reader, "SELECT i,k FROM t WHERE k>=0");
	REQUIRE_FALSE(stream->HasError());
	auto chunk = stream->Fetch();
	REQUIRE(chunk);

	// The bounded result buffer keeps scan assignments alive during checkpoint.
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM t WHERE i%7=0"));
	REQUIRE_NO_FAIL(writer.Query("INSERT INTO t SELECT i,i%97 FROM range(1228800,1228900) r(i)"));
	REQUIRE_NO_FAIL(writer.Query("CHECKPOINT"));

	const idx_t expected_rows = 1228800;
	vector<bool> seen(expected_rows, false);
	idx_t rows = 0;
	bool valid = true;
	do {
		for (idx_t row = 0; row < chunk->size(); row++) {
			const auto i = chunk->GetValue(0, row).GetValue<int64_t>();
			const auto k = chunk->GetValue(1, row).GetValue<int64_t>();
			if (i < 0 || idx_t(i) >= expected_rows) {
				valid = false;
				continue;
			}
			valid &= !seen[i] && k == i % 97;
			seen[i] = true;
			rows++;
		}
		chunk = stream->Fetch();
	} while (chunk);
	REQUIRE_FALSE(stream->HasError());
	REQUIRE(valid);
	REQUIRE(rows == expected_rows);
	stream.reset();
	REQUIRE_NO_FAIL(reader.Query("COMMIT"));

	REQUIRE_NO_FAIL(writer.Query("UPDATE t SET k=k+100 WHERE i%5=0 AND i<1228800"));
	REQUIRE_NO_FAIL(writer.Query("CHECKPOINT"));
	auto result = reader.Query("SELECT count(*),sum(k)::BIGINT FROM t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1053357}));
	REQUIRE(CHECK_COLUMN(result, 1, {71625956}));
}

TEST_CASE("Native scan assignments retain transaction-local rows across concurrent appends", "[api][storage]") {
	auto path = TestCreatePath("native_scan_local_rows.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection reader(db), writer(db);
	REQUIRE_NO_FAIL(writer.Query("CREATE TABLE t AS SELECT i, 0::BIGINT marker FROM range(1228800) r(i)"));
	REQUIRE_NO_FAIL(writer.Query("CHECKPOINT"));
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(reader.Query("SET threads=4"));
#endif
	REQUIRE_NO_FAIL(reader.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(reader.Query("SET streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(reader.Query("SET storage_block_prefetch='never'"));
	REQUIRE_NO_FAIL(reader.Query("PRAGMA verify_parallelism"));
	REQUIRE_NO_FAIL(reader.Query("BEGIN"));
	REQUIRE_NO_FAIL(reader.Query("INSERT INTO t SELECT i, 1 FROM range(6144) r(i)"));

	auto stream = OpenStream(reader, "SELECT marker FROM t");
	REQUIRE_FALSE(stream->HasError());
	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE_NO_FAIL(writer.Query("INSERT INTO t SELECT i, 2 FROM range(122880) r(i)"));

	idx_t persistent_rows = 0;
	idx_t local_rows = 0;
	idx_t invisible_rows = 0;
	bool valid = true;
	do {
		for (auto marker : chunk->data[0].Values<int64_t>()) {
			if (!marker.IsValid()) {
				valid = false;
				continue;
			}
			switch (marker.GetValue()) {
			case 0:
				persistent_rows++;
				break;
			case 1:
				local_rows++;
				break;
			default:
				invisible_rows++;
				break;
			}
		}
		chunk = stream->Fetch();
	} while (chunk);
	REQUIRE_FALSE(stream->HasError());
	REQUIRE(valid);
	REQUIRE(persistent_rows == 1228800);
	REQUIRE(local_rows == 6144);
	REQUIRE(invisible_rows == 0);
	stream.reset();
	REQUIRE_NO_FAIL(reader.Query("ROLLBACK"));
	auto result = reader.Query("SELECT count(*), sum(marker)::BIGINT FROM t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1351680}));
	REQUIRE(CHECK_COLUMN(result, 1, {245760}));
}

TEST_CASE("Native scan progress counts consumed assignments and survives "
          "stream abandonment",
          "[api][storage]") {
	auto path = TestCreatePath("native_scan_progress.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i, i%97 k FROM range(2000000) r(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
#endif
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='never'"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar=true"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar_print=false"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA verify_parallelism"));

	auto progress = [&]() {
		ProgressData data;
		auto &executor = con.context->GetExecutor();
		REQUIRE_FALSE(executor.GetPipelinesProgress(data));
		REQUIRE(data.IsValid());
		if (data.total == 0) {
			REQUIRE(executor.ExecutionIsFinished());
			return 100.0;
		}
		return data.ProgressDone() * 100;
	};

	auto stream = OpenStream(con, "SELECT i,k FROM t WHERE k>=0");
	REQUIRE_FALSE(stream->HasError());
	idx_t rows = 0;
	idx_t partial_samples = 0;
	double previous = 0;
	while (auto chunk = stream->Fetch()) {
		rows += chunk->size();
		const auto current = progress();
		REQUIRE(current >= previous);
		REQUIRE(current <= 100);
		partial_samples += current > 0 && current < 100;
		previous = current;
	}
	REQUIRE_FALSE(stream->HasError());
	REQUIRE(rows == 2000000);
	REQUIRE(partial_samples > 1);
	REQUIRE(previous == 100);
	stream.reset();

	stream = OpenStream(con, "SELECT i,k FROM t WHERE k>=0");
	REQUIRE_FALSE(stream->HasError());
	auto first = stream->Fetch();
	REQUIRE(first);
	REQUIRE(first->size() > 0);
	REQUIRE(progress() < 20);
	stream.reset();

	auto result = con.Query("SELECT count(*),sum(i)::BIGINT FROM t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {2000000}));
	REQUIRE(CHECK_COLUMN(result, 1, {int64_t(1999999000000)}));
}

TEST_CASE("Native scan progress survives repeated pipeline source reset", "[api][storage]") {
	auto path = TestCreatePath("native_scan_progress_reset.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i FROM range(100003) r(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar=true"));
	REQUIRE_NO_FAIL(con.Query("SET enable_progress_bar_print=false"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET storage_block_prefetch='never'"));
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
#endif
	auto result = con.Query("WITH RECURSIVE r(n,v) AS (VALUES (0,0::HUGEINT) UNION ALL "
	                        "SELECT n+1, (SELECT sum(i) FROM t WHERE i>=r.n) FROM r WHERE n<5) "
	                        "SELECT n,v FROM r ORDER BY n");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {int64_t(0), int64_t(5000250003), int64_t(5000250003), int64_t(5000250002),
	                      int64_t(5000250000), int64_t(5000249997)}));
}
