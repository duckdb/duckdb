#include "catch.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

namespace {

string ExportSnapshot(Connection &con) {
	auto result = con.Query("SELECT duckdb_export_transaction()");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	return result->GetValue(0, 0).ToString();
}

void AttachToSnapshot(Connection &con, const string &id) {
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("SET TRANSACTION SNAPSHOT '" + id + "'");
	REQUIRE_NO_FAIL(*r);
}

} // namespace

TEST_CASE("Shared transaction: participant sees owner's uncommitted writes", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1), (2), (3)"));

	auto snapshot_id = ExportSnapshot(owner);
	REQUIRE(!snapshot_id.empty());

	// without attaching, participant must NOT see uncommitted rows
	{
		auto r = participant.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 0);
	}

	AttachToSnapshot(participant, snapshot_id);

	// after attaching, participant sees owner's uncommitted rows
	{
		auto r = participant.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 3);
	}

	// participant detaches via COMMIT (no actual finalize)
	REQUIRE_NO_FAIL(participant.Query("COMMIT"));

	// owner can still ROLLBACK its writes
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	{
		auto r = owner.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 0);
	}
}

TEST_CASE("Shared transaction: participant writes are visible to owner", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// participant writes
	REQUIRE_NO_FAIL(participant.Query("INSERT INTO t VALUES (2)"));

	// owner sees participant's row
	{
		auto r = owner.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 2);
	}

	// participant detaches; owner commits
	REQUIRE_NO_FAIL(participant.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// new connection should now see both rows
	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 2);
}

TEST_CASE("Shared transaction: owner ROLLBACK force-detaches participants", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Owner ROLLBACK blocks until participants detach (symmetric with COMMIT). Run it
	// on a separate thread so we can observe the force-detach signal from the main
	// thread and then drive the participant's clean detach.
	std::atomic<bool> rollback_done {false};
	std::string rollback_error;
	std::thread rollback_thread([&]() {
		auto r = owner.Query("ROLLBACK");
		if (r->HasError()) {
			rollback_error = r->GetError();
		}
		rollback_done.store(true);
	});

	// Force-detach has been signalled (synchronously, before owner blocks on the cv),
	// so the participant's next storage access errors with the documented message.
	// Spin briefly to let the rollback thread reach ForceDetach.
	bool saw_force_detach_error = false;
	for (int i = 0; i < 100 && !rollback_done.load(); i++) {
		auto r = participant.Query("SELECT count(*) FROM t");
		if (r->HasError()) {
			saw_force_detach_error = true;
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	REQUIRE(saw_force_detach_error);

	// Auto-recovery: the participant should now be back in auto-commit mode without any
	// explicit ROLLBACK. Issuing a normal query both succeeds (proving auto-commit) and
	// the side effect of the previous failed query unblocked the owner's ROLLBACK
	// (proving the implicit detach happened during EndQueryInternal of the failed query).
	rollback_thread.join();
	REQUIRE(rollback_error.empty());
	REQUIRE(rollback_done.load());
	REQUIRE_NO_FAIL(participant.Query("SELECT 1"));
}

TEST_CASE("Shared transaction: invalid snapshot id errors", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("SET TRANSACTION SNAPSHOT '00000000-0000-0000-0000-000000000000'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: cannot export from auto-commit", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto r = con.Query("SELECT duckdb_export_transaction()");
	REQUIRE(r->HasError());
}

TEST_CASE("Shared transaction: cannot attach after work has been done in the transaction", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);

	REQUIRE_NO_FAIL(participant.Query("BEGIN"));
	// participant accesses a table — opens a per-database transaction. Must reject
	// the snapshot import after this.
	REQUIRE_NO_FAIL(participant.Query("SELECT count(*) FROM t"));

	auto r = participant.Query("SET TRANSACTION SNAPSHOT '" + snapshot_id + "'");
	REQUIRE(r->HasError());

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: cannot attach without BEGIN", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);

	// participant has no BEGIN — must error
	auto r = participant.Query("SET TRANSACTION SNAPSHOT '" + snapshot_id + "'");
	REQUIRE(r->HasError());

	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: ATTACH and DETACH are rejected for participants", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	{
		auto r = participant.Query("ATTACH ':memory:' AS extra");
		REQUIRE(r->HasError());
	}
	{
		auto r = participant.Query("DETACH extra");
		REQUIRE(r->HasError());
	}

	// Participant detaches; owner ROLLBACK can complete.
	std::string rollback_error;
	std::thread rollback_thread([&]() {
		auto r = owner.Query("ROLLBACK");
		if (r->HasError()) {
			rollback_error = r->GetError();
		}
	});
	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	rollback_thread.join();
	REQUIRE(rollback_error.empty());
}

TEST_CASE("Shared transaction: ATTACH and DETACH are also rejected for the owner while shared", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Owner is also inside the shared transaction — same restriction applies.
	{
		auto r = owner.Query("ATTACH ':memory:' AS extra");
		REQUIRE(r->HasError());
	}

	std::string rollback_error;
	std::thread rollback_thread([&]() {
		auto r = owner.Query("ROLLBACK");
		if (r->HasError()) {
			rollback_error = r->GetError();
		}
	});
	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	rollback_thread.join();
	REQUIRE(rollback_error.empty());
}

TEST_CASE("Shared transaction: cross-database read by participant", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);

	// Pre-attach a second database before any shared transaction starts.
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS extra"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE main.m(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE extra.x(j INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO extra.x VALUES (10), (20), (30)"));

	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO main.m VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Participant should see committed rows of the second database alongside the
	// owner's uncommitted main-db writes.
	{
		auto r = participant.Query("SELECT count(*) FROM extra.x");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 3);
	}
	{
		auto r = participant.Query("SELECT count(*) FROM main.m");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 1);
	}

	REQUIRE_NO_FAIL(participant.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transaction: late import during owner COMMIT is rejected", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection p1(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(p1, snapshot_id);

	// Owner COMMIT will block until p1 detaches. While it blocks, attempting to import
	// the snapshot from a fresh connection must be rejected — otherwise an adversarial
	// caller could keep re-attaching and starve the owner's wait indefinitely.
	std::atomic<bool> commit_done {false};
	std::string commit_error;
	std::thread commit_thread([&]() {
		auto r = owner.Query("COMMIT");
		if (r->HasError()) {
			commit_error = r->GetError();
		}
		commit_done.store(true);
	});

	// Spin until the commit thread has set the committing flag.
	Connection p_late(db);
	bool import_was_rejected = false;
	for (int i = 0; i < 100 && !commit_done.load(); i++) {
		REQUIRE_NO_FAIL(p_late.Query("BEGIN"));
		auto r = p_late.Query("SET TRANSACTION SNAPSHOT '" + snapshot_id + "'");
		if (r->HasError()) {
			import_was_rejected = true;
			REQUIRE_NO_FAIL(p_late.Query("ROLLBACK"));
			break;
		}
		// If the import succeeded, we lost the race — detach and try again.
		REQUIRE_NO_FAIL(p_late.Query("COMMIT"));
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	REQUIRE(import_was_rejected);

	// p1 detaches; owner's COMMIT unblocks.
	REQUIRE_NO_FAIL(p1.Query("COMMIT"));
	commit_thread.join();
	REQUIRE(commit_error.empty());
	REQUIRE(commit_done.load());

	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 1);
}

TEST_CASE("Shared transaction: concurrent participant writes are serialized", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection p1(db);
	Connection p2(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(p1, snapshot_id);
	AttachToSnapshot(p2, snapshot_id);

	// Both participants insert N rows concurrently. With the per-meta statement lock
	// in place, statement execution serializes across participants and the inserts
	// land cleanly without LocalStorage / UndoBuffer corruption. Without the lock,
	// this is the smell test that produces missing rows or crashes (the latter under
	// sanitizer builds).
	const idx_t kRowsPerWriter = 200;
	std::string p1_error;
	std::string p2_error;
	auto writer = [&](Connection &con, int base, std::string &error_out) {
		for (idx_t i = 0; i < kRowsPerWriter; i++) {
			auto r = con.Query("INSERT INTO t VALUES (" + std::to_string(base + (int)i) + ")");
			if (r->HasError()) {
				error_out = r->GetError();
				return;
			}
		}
	};
	std::thread t1(writer, std::ref(p1), 0, std::ref(p1_error));
	std::thread t2(writer, std::ref(p2), 1000, std::ref(p2_error));
	t1.join();
	t2.join();
	REQUIRE(p1_error.empty());
	REQUIRE(p2_error.empty());

	// All rows are visible inside the shared transaction.
	{
		auto r = owner.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == int64_t(2 * kRowsPerWriter));
	}

	REQUIRE_NO_FAIL(p1.Query("COMMIT"));
	REQUIRE_NO_FAIL(p2.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// And persisted after the owner commits.
	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == int64_t(2 * kRowsPerWriter));
}

TEST_CASE("Shared transaction: owner COMMIT waits for participants", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (42)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	std::atomic<bool> commit_done {false};
	std::string commit_error;
	std::thread commit_thread([&]() {
		auto r = owner.Query("COMMIT");
		if (r->HasError()) {
			commit_error = r->GetError();
		}
		commit_done.store(true);
	});

	// give the commit thread time to start waiting
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE_FALSE(commit_done.load());

	// detach participant; owner COMMIT should now complete
	REQUIRE_NO_FAIL(participant.Query("COMMIT"));

	commit_thread.join();
	REQUIRE(commit_error.empty());
	REQUIRE(commit_done.load());

	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 1);
}
