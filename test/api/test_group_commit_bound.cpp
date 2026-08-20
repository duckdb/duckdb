#include "catch.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb;

// These tests pin the visibility bound group commit relies on: a commit published before its WAL
// is durable must not be observed by a transaction that starts in that window.
// debug_wal_fsync_sleep_ms parks the writer in its fsync so the interleaving is deterministic
namespace {

constexpr idx_t FSYNC_MS = 4000;
constexpr idx_t READER_DELAY_MS = 500;
constexpr idx_t ROW_COUNT = 5000;

void SleepMs(idx_t ms) {
	std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

idx_t ScalarValue(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<idx_t>();
}

} // namespace

TEST_CASE("A commit pending durability is not visible to a transaction that starts after it", "[api][group_commit]") {
	auto db_path = TestCreatePath("group_commit_bound.db");
	DeleteDatabase(db_path);
	DuckDB db(db_path);

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE scratch(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("SET debug_wal_fsync_sleep_ms=" + to_string(FSYNC_MS)));

	// the writer publishes its rows and then parks in the WAL fsync for FSYNC_MS
	bool writer_failed = false;
	std::thread writer([&db, &writer_failed]() {
		Connection con(db);
		writer_failed = con.Query("INSERT INTO t SELECT * FROM range(" + to_string(ROW_COUNT) + ")")->HasError();
	});

	SleepMs(READER_DELAY_MS);

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

	writer.join();
	REQUIRE(!writer_failed);

	// once the commit is acknowledged it is durable, so a fresh transaction observes it
	REQUIRE(ScalarValue(setup, "SELECT count(*) FROM t") == ROW_COUNT);
}

TEST_CASE("A bounded transaction conflicts with the commit it cannot see", "[api][group_commit]") {
	auto db_path = TestCreatePath("group_commit_bound_conflict.db");
	DeleteDatabase(db_path);
	DuckDB db(db_path);

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO t VALUES (1, 0)"));
	REQUIRE_NO_FAIL(setup.Query("SET debug_wal_fsync_sleep_ms=" + to_string(FSYNC_MS)));

	// the writer's update is published, then parked in the fsync
	bool writer_failed = false;
	std::thread writer([&db, &writer_failed]() {
		Connection con(db);
		writer_failed = con.Query("UPDATE t SET v = 1 WHERE i = 1")->HasError();
	});

	SleepMs(READER_DELAY_MS);

	// this transaction's snapshot is bounded AT the writer's commit id, so it does not see the
	// update - but it must still conflict with it rather than silently overwriting it
	Connection writer2(db);
	REQUIRE_NO_FAIL(writer2.Query("BEGIN"));
	REQUIRE(ScalarValue(writer2, "SELECT v FROM t WHERE i = 1") == 0);
	auto conflict = writer2.Query("UPDATE t SET v = 2 WHERE i = 1");
	REQUIRE(conflict->HasError());
	REQUIRE_NO_FAIL(writer2.Query("ROLLBACK"));

	writer.join();
	REQUIRE(!writer_failed);

	// the writer's update is the one that survives
	REQUIRE(ScalarValue(setup, "SELECT v FROM t WHERE i = 1") == 1);
}

TEST_CASE("A checkpoint waiting for durability can be interrupted", "[api][group_commit]") {
	auto db_path = TestCreatePath("group_commit_bound_interrupt.db");
	DeleteDatabase(db_path);
	DuckDB db(db_path);

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("SET debug_wal_fsync_sleep_ms=" + to_string(FSYNC_MS)));

	bool writer_failed = false;
	std::thread writer([&db, &writer_failed]() {
		Connection con(db);
		writer_failed = con.Query("INSERT INTO t SELECT * FROM range(" + to_string(ROW_COUNT) + ")")->HasError();
	});

	SleepMs(READER_DELAY_MS);

	// the checkpoint waits on another connection's fsync - that wait must stay cancellable
	Connection checkpointer(db);
	auto start = std::chrono::steady_clock::now();
	// interrupt repeatedly: a single signal can land before the statement reaches the wait
	std::atomic<bool> stop_interrupting(false);
	std::thread interrupter([&checkpointer, &stop_interrupting]() {
		SleepMs(READER_DELAY_MS);
		while (!stop_interrupting) {
			checkpointer.Interrupt();
			SleepMs(25);
		}
	});
	auto result = checkpointer.Query("FORCE CHECKPOINT");
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
	stop_interrupting = true;
	interrupter.join();
	writer.join();
	REQUIRE(!writer_failed);

	REQUIRE(result->HasError());
	// without a cancellable drain this waits out the writer's fsync and then performs its own
	REQUIRE(idx_t(elapsed.count()) < FSYNC_MS);
}

TEST_CASE("ALTER TYPE keeps committed updates it cannot see in its own snapshot", "[api][group_commit]") {
	auto db_path = TestCreatePath("group_commit_bound_alter.db");
	DeleteDatabase(db_path);
	DuckDB db(db_path);

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO t VALUES (1, 0)"));
	REQUIRE_NO_FAIL(setup.Query("SET debug_wal_fsync_sleep_ms=" + to_string(FSYNC_MS)));

	bool writer_failed = false;
	std::thread writer([&db, &writer_failed]() {
		Connection con(db);
		writer_failed = con.Query("UPDATE t SET v = 7 WHERE i = 1")->HasError();
	});

	SleepMs(READER_DELAY_MS);

	// the retype runs on a snapshot bounded below the update, but the new column does not inherit
	// the old column's update segments - so it must bake the committed value, not the stale one
	Connection alterer(db);
	auto altered = alterer.Query("ALTER TABLE t ALTER COLUMN v SET DATA TYPE BIGINT");

	writer.join();
	REQUIRE(!writer_failed);

	if (!altered->HasError()) {
		REQUIRE(ScalarValue(setup, "SELECT v FROM t WHERE i = 1") == 7);
	}
}

TEST_CASE("txid_current stays unique while commits are pending durability", "[api][group_commit]") {
	auto db_path = TestCreatePath("group_commit_bound_txid.db");
	DeleteDatabase(db_path);
	DuckDB db(db_path);

	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET checkpoint_threshold='1TB'"));
	REQUIRE_NO_FAIL(setup.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("SET debug_wal_fsync_sleep_ms=" + to_string(FSYNC_MS)));

	bool writer_failed = false;
	std::thread writer([&db, &writer_failed]() {
		Connection con(db);
		writer_failed = con.Query("INSERT INTO t SELECT * FROM range(" + to_string(ROW_COUNT) + ")")->HasError();
	});

	SleepMs(READER_DELAY_MS);

	// both transactions are capped at the same commit, so they share a snapshot start time - the id
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

	writer.join();
	REQUIRE(!writer_failed);
}
