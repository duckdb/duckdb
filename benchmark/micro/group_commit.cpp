#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"

#include <chrono>
#include <thread>
#include <vector>

using namespace duckdb;

// A LocalFileSystem that makes each WAL fsync cost a fixed delay, emulating
// high-latency durable storage (e.g. networked disks like EFS). With delay 0 the
// real fsync is performed. This exposes the WAL group-commit benefit, which grows
// as the fsync gets slower and more concurrent commits batch into each one.
class DelayFsyncFileSystem : public LocalFileSystem {
public:
	explicit DelayFsyncFileSystem(int64_t delay_us_p) : delay_us(delay_us_p) {
	}
	void FileSync(FileHandle &handle) override {
		if (delay_us > 0 && StringUtil::EndsWith(handle.GetPath(), ".wal")) {
			std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
			return;
		}
		LocalFileSystem::FileSync(handle);
	}

private:
	int64_t delay_us;
};

// Owns a persistent database whose WAL fsync latency is simulated. The base
// DuckDBBenchmarkState db/conn (in-memory) are unused; all work runs on gc_db.
struct GroupCommitState : public DuckDBBenchmarkState {
	duckdb::unique_ptr<DuckDB> gc_db;

	explicit GroupCommitState(int64_t delay_us) : DuckDBBenchmarkState(string()) {
		DBConfig config;
		config.file_system = make_uniq<VirtualFileSystem>(make_uniq<DelayFsyncFileSystem>(delay_us));
		string path = "duckdb_group_commit_bench.db";
		DeleteDatabase(path);
		gc_db = make_uniq<DuckDB>(path, &config);
		Connection con(*gc_db);
		// keep the whole run on the WAL path - do not checkpoint mid-run
		con.Query("SET checkpoint_threshold='1TB'");
	}
};

// NUM_THREADS connections each commit COMMITS_PER_THREAD single-row INSERT
// transactions. Every auto-commit INSERT is its own transaction, so this exercises
// the group-commit path where concurrent committers batch their WAL fsyncs.
#define GROUP_COMMIT_BENCHMARK(NUM_THREADS, COMMITS_PER_THREAD, DELAY_US)                                              \
	duckdb::unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                         \
		return make_uniq<GroupCommitState>(DELAY_US);                                                                  \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state_p) override {                                                                \
		auto state = (GroupCommitState *)state_p;                                                                      \
		Connection con(*state->gc_db);                                                                                 \
		con.Query("CREATE TABLE integers(i INTEGER, t INTEGER)");                                                      \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state_p) override {                                                        \
		auto state = (GroupCommitState *)state_p;                                                                      \
		std::vector<std::thread> threads;                                                                              \
		for (int64_t t = 0; t < NUM_THREADS; t++) {                                                                    \
			threads.emplace_back([state, t]() {                                                                        \
				Connection con(*state->gc_db);                                                                         \
				for (int64_t i = 0; i < COMMITS_PER_THREAD; i++) {                                                     \
					con.Query("INSERT INTO integers VALUES (" + std::to_string(i) + ", " + std::to_string(t) + ")");   \
				}                                                                                                      \
			});                                                                                                        \
		}                                                                                                              \
		for (auto &thread : threads) {                                                                                 \
			thread.join();                                                                                             \
		}                                                                                                              \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state_p) override {                                                             \
		auto state = (GroupCommitState *)state_p;                                                                      \
		Connection con(*state->gc_db);                                                                                 \
		con.Query("DROP TABLE integers");                                                                              \
		con.Query("CREATE TABLE integers(i INTEGER, t INTEGER)");                                                      \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return std::to_string((int64_t)NUM_THREADS) + " threads commit " +                                             \
		       std::to_string((int64_t)(NUM_THREADS * COMMITS_PER_THREAD)) + " INSERT transactions, " +                \
		       std::to_string((int64_t)DELAY_US) + "us simulated WAL fsync latency (group commit)";                    \
	}

// real local fsync (fixed 16000 total commits)
DUCKDB_BENCHMARK(GroupCommit1Thread, "[wal]")
GROUP_COMMIT_BENCHMARK(1, 16000, 0)
FINISH_BENCHMARK(GroupCommit1Thread)

DUCKDB_BENCHMARK(GroupCommit4Threads, "[wal]")
GROUP_COMMIT_BENCHMARK(4, 4000, 0)
FINISH_BENCHMARK(GroupCommit4Threads)

DUCKDB_BENCHMARK(GroupCommit8Threads, "[wal]")
GROUP_COMMIT_BENCHMARK(8, 2000, 0)
FINISH_BENCHMARK(GroupCommit8Threads)

DUCKDB_BENCHMARK(GroupCommit16Threads, "[wal]")
GROUP_COMMIT_BENCHMARK(16, 1000, 0)
FINISH_BENCHMARK(GroupCommit16Threads)

// 1ms simulated fsync latency (fixed 800 total commits)
DUCKDB_BENCHMARK(GroupCommit1Thread1ms, "[wal]")
GROUP_COMMIT_BENCHMARK(1, 800, 1000)
FINISH_BENCHMARK(GroupCommit1Thread1ms)

DUCKDB_BENCHMARK(GroupCommit4Threads1ms, "[wal]")
GROUP_COMMIT_BENCHMARK(4, 200, 1000)
FINISH_BENCHMARK(GroupCommit4Threads1ms)

DUCKDB_BENCHMARK(GroupCommit8Threads1ms, "[wal]")
GROUP_COMMIT_BENCHMARK(8, 100, 1000)
FINISH_BENCHMARK(GroupCommit8Threads1ms)

DUCKDB_BENCHMARK(GroupCommit16Threads1ms, "[wal]")
GROUP_COMMIT_BENCHMARK(16, 50, 1000)
FINISH_BENCHMARK(GroupCommit16Threads1ms)

// 10ms simulated fsync latency (fixed 160 total commits)
DUCKDB_BENCHMARK(GroupCommit1Thread10ms, "[wal]")
GROUP_COMMIT_BENCHMARK(1, 160, 10000)
FINISH_BENCHMARK(GroupCommit1Thread10ms)

DUCKDB_BENCHMARK(GroupCommit4Threads10ms, "[wal]")
GROUP_COMMIT_BENCHMARK(4, 40, 10000)
FINISH_BENCHMARK(GroupCommit4Threads10ms)

DUCKDB_BENCHMARK(GroupCommit8Threads10ms, "[wal]")
GROUP_COMMIT_BENCHMARK(8, 20, 10000)
FINISH_BENCHMARK(GroupCommit8Threads10ms)

DUCKDB_BENCHMARK(GroupCommit16Threads10ms, "[wal]")
GROUP_COMMIT_BENCHMARK(16, 10, 10000)
FINISH_BENCHMARK(GroupCommit16Threads10ms)
