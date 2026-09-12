#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"

#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

// These tests pin the visibility bound group commit relies on: a commit published before its WAL
// is durable must not be observed by a transaction that starts in that window. A gated file system
// parks the writer inside its WAL fsync, so the interleaving is deterministic
namespace {

constexpr idx_t ROW_COUNT = 5000;

//! Parks WAL fsyncs while armed, holding a commit between publishing and becoming durable
class GatedFsyncFileSystem : public LocalFileSystem {
public:
	void FileSync(FileHandle &handle) override {
		if (StringUtil::Contains(handle.GetPath(), ".wal")) {
			unique_lock<mutex> guard(lock);
			if (armed) {
				parked = true;
				cv.notify_all();
				cv.wait(guard, [&]() { return released; });
			}
		}
		LocalFileSystem::FileSync(handle);
	}
	//! Park the next WAL fsync
	void Arm() {
		lock_guard<mutex> guard(lock);
		armed = true;
		parked = false;
		released = false;
	}
	//! Wait until a WAL fsync is parked: its commit is published but not durable
	bool WaitUntilParked() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(60), [&]() { return parked; });
	}
	//! Let the parked fsync, and every later one, proceed
	void Release() {
		{
			lock_guard<mutex> guard(lock);
			released = true;
			armed = false;
		}
		cv.notify_all();
	}

private:
	mutex lock;
	std::condition_variable cv;
	bool armed = false;
	bool parked = false;
	bool released = false;
};

//! A database on the gated file system, plus a writer thread that is parked in its WAL fsync
struct GatedDatabase {
	explicit GatedDatabase(const string &name) {
		auto path = TestCreatePath(name);
		DeleteDatabase(path);
		auto fs = make_uniq<GatedFsyncFileSystem>();
		gate = fs.get();
		DBConfig config;
		config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));
		db = make_uniq<DuckDB>(path, &config);
	}
	~GatedDatabase() {
		// a failed REQUIRE must not leave the writer parked, or a joinable thread would terminate
		FinishWriter();
	}

	//! Run the query on its own connection and wait until its commit is parked in the WAL fsync
	void StartWriter(const string &query) {
		// a commit with no other transaction syncs under the transaction lock; keep one open, touching
		// the database so it really starts, and the writer defers its sync instead
		holder = make_uniq<Connection>(*db);
		REQUIRE_NO_FAIL(holder->Query("BEGIN"));
		REQUIRE_NO_FAIL(holder->Query("SELECT count(*) FROM t"));
		gate->Arm();
		writer = std::thread([this, query]() {
			Connection con(*db);
			writer_failed = con.Query(query)->HasError();
		});
		REQUIRE(gate->WaitUntilParked());
	}
	//! Release the writer and wait for its commit to be acknowledged; returns whether it succeeded
	bool FinishWriter() {
		gate->Release();
		if (writer.joinable()) {
			writer.join();
		}
		if (holder) {
			holder->Query("ROLLBACK");
			holder.reset();
		}
		return !writer_failed;
	}

	unique_ptr<DuckDB> db;
	GatedFsyncFileSystem *gate;
	unique_ptr<Connection> holder;
	std::thread writer;
	bool writer_failed = false;
};

idx_t ScalarValue(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<idx_t>();
}

} // namespace

TEST_CASE("A commit pending durability is not visible to a transaction that starts after it", "[api][group_commit]") {
	GatedDatabase gated("group_commit_bound.db");
	auto &db = *gated.db;

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE scratch(i INTEGER)"));

	// the writer publishes its rows and is then parked in the WAL fsync
	gated.StartWriter("INSERT INTO t SELECT * FROM range(" + to_string(ROW_COUNT) + ")");

	// published but not yet durable: a transaction starting now is bounded below it and must see
	// the pre-insert state, twice in a row - its snapshot cannot shift under it
	Connection reader(db);
	REQUIRE_NO_FAIL(reader.Query("BEGIN"));
	REQUIRE(ScalarValue(reader, "SELECT count(*) FROM t") == 0);

	// a rollback drains the cleanup queue without waiting for any fsync, so the writer's queued
	// cleanup runs here - it must not discard the version info that hides the pending rows
	Connection drainer(db);
	REQUIRE_NO_FAIL(drainer.Query("BEGIN"));
	REQUIRE_NO_FAIL(drainer.Query("INSERT INTO scratch VALUES (1)"));
	REQUIRE_NO_FAIL(drainer.Query("ROLLBACK"));

	REQUIRE(ScalarValue(reader, "SELECT count(*) FROM t") == 0);
	REQUIRE_NO_FAIL(reader.Query("COMMIT"));

	REQUIRE(gated.FinishWriter());

	// once the commit is acknowledged it is durable, so a fresh transaction observes it
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == ROW_COUNT);
}

TEST_CASE("A bounded transaction conflicts with the commit it cannot see", "[api][group_commit]") {
	GatedDatabase gated("group_commit_bound_conflict.db");
	auto &db = *gated.db;

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO t VALUES (1, 0)"));

	// the writer's update is published, then parked in the fsync
	gated.StartWriter("UPDATE t SET v = 1 WHERE i = 1");

	// this transaction's snapshot is bounded AT the writer's commit id, so it does not see the
	// update - but it must still conflict with it rather than silently overwriting it
	Connection writer2(db);
	REQUIRE_NO_FAIL(writer2.Query("BEGIN"));
	REQUIRE(ScalarValue(writer2, "SELECT v FROM t WHERE i = 1") == 0);
	auto conflict = writer2.Query("UPDATE t SET v = 2 WHERE i = 1");
	REQUIRE(conflict->HasError());
	REQUIRE_NO_FAIL(writer2.Query("ROLLBACK"));

	REQUIRE(gated.FinishWriter());

	// the writer's update is the one that survives
	REQUIRE(ScalarValue(setup, "SELECT v FROM t WHERE i = 1") == 1);
}

TEST_CASE("txid_current stays unique while commits are pending durability", "[api][group_commit]") {
	GatedDatabase gated("group_commit_bound_txid.db");
	auto &db = *gated.db;

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER)"));

	gated.StartWriter("INSERT INTO t SELECT * FROM range(" + to_string(ROW_COUNT) + ")");

	// both transactions are capped at the same commit, so they share a visibility bound - the id
	// reported to the user must still be the distinct one each of them drew
	Connection first(db);
	Connection second(db);
	REQUIRE_NO_FAIL(first.Query("BEGIN"));
	REQUIRE_NO_FAIL(second.Query("BEGIN"));
	REQUIRE(ScalarValue(first, "SELECT count(*) FROM t") == 0);
	REQUIRE(ScalarValue(second, "SELECT count(*) FROM t") == 0);
	REQUIRE(ScalarValue(first, "SELECT txid_current()") != ScalarValue(second, "SELECT txid_current()"));
	REQUIRE_NO_FAIL(first.Query("COMMIT"));
	REQUIRE_NO_FAIL(second.Query("COMMIT"));

	REQUIRE(gated.FinishWriter());
}
