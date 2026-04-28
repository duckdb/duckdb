#include "catch.hpp"
#include "test_helpers.hpp"

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

	// participant detaches via COMMIT; underlying txn keeps living because owner still holds.
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

	// participant detaches; owner COMMITs (last detacher actually finalizes).
	REQUIRE_NO_FAIL(participant.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// new connection should now see both rows
	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 2);
}

TEST_CASE("Shared transaction: any rollback dooms the transaction (eager fail on COMMIT)", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Participant ROLLBACK sets the doom flag and detaches.
	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));

	// Owner's subsequent COMMIT must fail eagerly.
	auto r = owner.Query("COMMIT");
	REQUIRE(r->HasError());

	// And the rows must not be visible to a fresh reader (last detacher rolled back).
	Connection fresh(db);
	auto r2 = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r2);
	REQUIRE(r2->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Shared transaction: owner ROLLBACK first, participant COMMIT fails eagerly", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Owner detaches via ROLLBACK first. No wait, no force-detach — just sets the doom flag.
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	// Participant's COMMIT must fail eagerly.
	auto r = participant.Query("COMMIT");
	REQUIRE(r->HasError());

	Connection fresh(db);
	auto r2 = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r2);
	REQUIRE(r2->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Shared transaction: invalid snapshot id errors", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("SET TRANSACTION SNAPSHOT '999999/missing'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: malformed snapshot id errors", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("SET TRANSACTION SNAPSHOT 'not-a-valid-id'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: owner connection close while shared dooms the transaction", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection participant(db);

	REQUIRE_NO_FAIL(participant.Query("CREATE TABLE t(i INTEGER)"));

	string snapshot_id;
	{
		Connection owner(db);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));
		snapshot_id = ExportSnapshot(owner);
		AttachToSnapshot(participant, snapshot_id);
	} // owner is destroyed here -> ~ClientContext -> implicit Rollback -> rollback_requested set

	// Participant's COMMIT must fail eagerly (the txn is doomed).
	auto r = participant.Query("COMMIT");
	REQUIRE(r->HasError());

	Connection fresh(db);
	auto r2 = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r2);
	REQUIRE(r2->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Shared transaction: DETACH of an unrelated database is allowed while sharing", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);

	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS unrelated"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE main.m(i INTEGER)"));

	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO main.m VALUES (1)"));
	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Detaching the unrelated DB while a shared txn exists on main must succeed.
	REQUIRE_NO_FAIL(participant.Query("DETACH unrelated"));

	// Detaching main (the shared one) must still be rejected.
	{
		auto r = participant.Query("DETACH main");
		REQUIRE(r->HasError());
	}

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: cannot export from auto-commit", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto r = con.Query("SELECT duckdb_export_transaction()");
	REQUIRE(r->HasError());
}

TEST_CASE("Shared transaction: cannot import own snapshot", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(con);
	auto r = con.Query("SET TRANSACTION SNAPSHOT '" + snapshot_id + "'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
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

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: ATTACH is also rejected for the owner while shared", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	{
		auto r = owner.Query("ATTACH ':memory:' AS extra");
		REQUIRE(r->HasError());
	}

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Shared transaction: cross-database read by participant", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);

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

TEST_CASE("Shared transaction: late import after all participants finalized is rejected", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto snapshot_id = ExportSnapshot(owner);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Owner is no longer in a transaction, so the snapshot id is stale.
	Connection late(db);
	REQUIRE_NO_FAIL(late.Query("BEGIN"));
	auto r = late.Query("SET TRANSACTION SNAPSHOT '" + snapshot_id + "'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(late.Query("ROLLBACK"));
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

	// Both participants insert N rows concurrently. With the per-DuckTransaction statement
	// lock in place, statement execution serializes across participants and the inserts
	// land cleanly without LocalStorage / UndoBuffer corruption.
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

	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == int64_t(2 * kRowsPerWriter));
}

TEST_CASE("Shared transaction: owner COMMIT first, participant COMMIT finalizes", "[shared_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (42)"));

	auto snapshot_id = ExportSnapshot(owner);
	AttachToSnapshot(participant, snapshot_id);

	// Owner detaches first via COMMIT — does NOT block (no waiting in v2).
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Until the last detacher (participant) commits, no fresh reader sees the rows.
	{
		Connection mid(db);
		auto r = mid.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 0);
	}

	REQUIRE_NO_FAIL(participant.Query("COMMIT"));

	Connection fresh(db);
	auto r = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r);
	REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 1);
}
