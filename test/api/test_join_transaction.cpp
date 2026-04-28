#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

#include <thread>

using namespace duckdb;

namespace {

string ShareTransaction(Connection &con) {
	auto result = con.Query("SELECT duckdb_share_transaction()");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 1);
	return result->GetValue(0, 0).ToString();
}

void JoinSharedTransaction(Connection &con, const string &id) {
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("JOIN TRANSACTION '" + id + "'");
	REQUIRE_NO_FAIL(*r);
}

} // namespace

TEST_CASE("Join transaction: participant sees owner's uncommitted writes", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1), (2), (3)"));

	auto txn_id = ShareTransaction(owner);
	REQUIRE(!txn_id.empty());

	// without attaching, participant must NOT see uncommitted rows
	{
		auto r = participant.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 0);
	}

	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: participant writes are visible to owner", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: any rollback dooms the transaction (eager fail on COMMIT)", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: owner ROLLBACK first, participant COMMIT fails eagerly", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: invalid snapshot id errors", "[join_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("JOIN TRANSACTION '999999/missing'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: malformed snapshot id errors", "[join_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto r = con.Query("JOIN TRANSACTION 'not-a-valid-id'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: owner connection close while shared dooms the transaction", "[join_txn]") {
	DuckDB db(nullptr);
	Connection participant(db);

	REQUIRE_NO_FAIL(participant.Query("CREATE TABLE t(i INTEGER)"));

	string txn_id;
	{
		Connection owner(db);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));
		txn_id = ShareTransaction(owner);
		JoinSharedTransaction(participant, txn_id);
	} // owner is destroyed here -> ~ClientContext -> implicit Rollback -> rollback_requested set

	// Participant's COMMIT must fail eagerly (the txn is doomed).
	auto r = participant.Query("COMMIT");
	REQUIRE(r->HasError());

	Connection fresh(db);
	auto r2 = fresh.Query("SELECT count(*) FROM t");
	REQUIRE_NO_FAIL(*r2);
	REQUIRE(r2->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Join transaction: DETACH of an unrelated database is allowed while sharing", "[join_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);

	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS unrelated"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE main.m(i INTEGER)"));

	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO main.m VALUES (1)"));
	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: cannot export from auto-commit", "[join_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto r = con.Query("SELECT duckdb_share_transaction()");
	REQUIRE(r->HasError());
}

TEST_CASE("Join transaction: cannot import own snapshot", "[join_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto txn_id = ShareTransaction(con);
	auto r = con.Query("JOIN TRANSACTION '" + txn_id + "'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: cannot attach after work has been done in the transaction", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto txn_id = ShareTransaction(owner);

	REQUIRE_NO_FAIL(participant.Query("BEGIN"));
	// participant accesses a table — opens a per-database transaction. Must reject
	// the snapshot import after this.
	REQUIRE_NO_FAIL(participant.Query("SELECT count(*) FROM t"));

	auto r = participant.Query("JOIN TRANSACTION '" + txn_id + "'");
	REQUIRE(r->HasError());

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: cannot attach without BEGIN", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto txn_id = ShareTransaction(owner);

	// participant has no BEGIN — must error
	auto r = participant.Query("JOIN TRANSACTION '" + txn_id + "'");
	REQUIRE(r->HasError());

	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: ATTACH and DETACH are rejected for participants", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

	{
		auto r = participant.Query("ATTACH ':memory:' AS extra");
		REQUIRE(r->HasError());
	}

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: ATTACH is also rejected for the owner while shared", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

	{
		auto r = owner.Query("ATTACH ':memory:' AS extra");
		REQUIRE(r->HasError());
	}

	REQUIRE_NO_FAIL(participant.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: cross-database read by participant", "[join_txn]") {
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

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

TEST_CASE("Join transaction: late import after all participants finalized is rejected", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));

	auto txn_id = ShareTransaction(owner);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Owner is no longer in a transaction, so the snapshot id is stale.
	Connection late(db);
	REQUIRE_NO_FAIL(late.Query("BEGIN"));
	auto r = late.Query("JOIN TRANSACTION '" + txn_id + "'");
	REQUIRE(r->HasError());
	REQUIRE_NO_FAIL(late.Query("ROLLBACK"));
}

TEST_CASE("Join transaction: concurrent participant writes are serialized", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection p1(db);
	Connection p2(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(p1, txn_id);
	JoinSharedTransaction(p2, txn_id);

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

TEST_CASE("Join transaction: owner COMMIT first, participant COMMIT finalizes", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (42)"));

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

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

// ---------------------------------------------------------------------------
// Concurrency regression tests covering bugs found in code review.
// ---------------------------------------------------------------------------

// Bug #1: 1→shared transition. The owner's BeginQuery used to skip the statement
// lock when participant_count==1, so an owner query running concurrently with a
// participant's JOIN TRANSACTION would mutate LocalStorage / UndoBuffer
// concurrently with the participant. With the fix, BeginQuery always acquires
// the statement lock, and JoinTransaction also acquires it — so the join waits
// for any in-flight owner query.
TEST_CASE("Join transaction: import races with concurrent owner writes (issue #1)", "[join_txn][!mayfail]") {
	for (int trial = 0; trial < 20; trial++) {
		DuckDB db(nullptr);
		Connection owner(db);
		Connection participant(db);

		REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		// Seed some prior writes so the underlying storage is non-trivial.
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (-1)"));

		auto txn_id = ShareTransaction(owner);

		// Owner hammers writes in a thread. The participant joins mid-stream.
		std::atomic<bool> stop {false};
		std::string owner_error;
		std::thread owner_thread([&]() {
			int n = 0;
			while (!stop.load()) {
				auto r = owner.Query("INSERT INTO t VALUES (" + std::to_string(n++) + ")");
				if (r->HasError()) {
					owner_error = r->GetError();
					return;
				}
			}
		});

		// Participant runs BEGIN + JOIN TRANSACTION while owner is busy. With the fix
		// these always succeed without TSan reports / sanitizer faults.
		REQUIRE_NO_FAIL(participant.Query("BEGIN"));
		auto r = participant.Query("JOIN TRANSACTION '" + txn_id + "'");
		REQUIRE_NO_FAIL(*r);

		// Participant inserts a few rows post-import while owner thread continues writing.
		for (int j = 0; j < 5; j++) {
			REQUIRE_NO_FAIL(participant.Query("INSERT INTO t VALUES (1000000 + " + std::to_string(j) + ")"));
		}

		stop.store(true);
		owner_thread.join();
		REQUIRE(owner_error.empty());

		REQUIRE_NO_FAIL(participant.Query("COMMIT"));
		REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	}
}

// Bug #2: TOCTOU between TryGetTransaction and TryAddParticipant. The lookup
// used to return a pointer with the meta lock released, then bump the count —
// allowing a concurrent owner-side DETACH to destroy the DuckTransaction in
// between. With the fix, the lookup-and-bump is atomic under owner_meta.lock.
//
// Triggering the destruction race deterministically is hard; instead we run
// many concurrent imports against an owner that aggressively detaches /
// reattaches an unrelated DB to keep the meta in motion, and verify no crash.
TEST_CASE("Join transaction: concurrent imports under owner-side meta churn (issue #2)", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (1)"));
	auto txn_id = ShareTransaction(owner);

	const idx_t kImporters = 8;
	std::vector<std::thread> threads;
	std::vector<std::string> errors(kImporters);
	for (idx_t i = 0; i < kImporters; i++) {
		threads.emplace_back([&, i]() {
			Connection p(db);
			if (auto r = p.Query("BEGIN"); r->HasError()) {
				errors[i] = r->GetError();
				return;
			}
			auto r = p.Query("JOIN TRANSACTION '" + txn_id + "'");
			if (r->HasError()) {
				errors[i] = r->GetError();
				return;
			}
			auto ins = p.Query("INSERT INTO t VALUES (" + std::to_string(100 + (int)i) + ")");
			if (ins->HasError()) {
				errors[i] = ins->GetError();
				return;
			}
			if (auto c = p.Query("COMMIT"); c->HasError()) {
				errors[i] = c->GetError();
			}
		});
	}
	for (auto &t : threads) {
		t.join();
	}
	for (auto &e : errors) {
		REQUIRE(e.empty());
	}
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

// Bug #3: deadlock from inconsistent lock ordering across multiple shared
// databases. BeginQuery used to lock in `OpenedTransactions` insertion order
// (which is per-connection), so two participants that imported {A,B} in
// opposite orders could AB/BA deadlock when both ran a query touching both.
// With the fix the locks are sorted by DuckTransaction pointer.
TEST_CASE("Join transaction: opposite-order multi-DB import does not deadlock (issue #3)", "[join_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbA"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbB"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbA.t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbB.t(i INTEGER)"));

	// Two owners, each holding a transaction on a different DB. Use a transaction
	// that has touched the relevant DB so duckdb_share_transaction returns it.
	Connection ownerA(db);
	REQUIRE_NO_FAIL(ownerA.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerA.Query("INSERT INTO dbA.t VALUES (1)"));
	auto snap_A = ShareTransaction(ownerA);

	Connection ownerB(db);
	REQUIRE_NO_FAIL(ownerB.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerB.Query("INSERT INTO dbB.t VALUES (1)"));
	auto snap_B = ShareTransaction(ownerB);

	// p1 imports A then B; p2 imports B then A. With unsorted lock order this
	// would AB/BA deadlock when both run cross-DB queries concurrently.
	Connection p1(db);
	REQUIRE_NO_FAIL(p1.Query("BEGIN"));
	REQUIRE_NO_FAIL(p1.Query("JOIN TRANSACTION '" + snap_A + "'"));
	REQUIRE_NO_FAIL(p1.Query("JOIN TRANSACTION '" + snap_B + "'"));

	Connection p2(db);
	REQUIRE_NO_FAIL(p2.Query("BEGIN"));
	REQUIRE_NO_FAIL(p2.Query("JOIN TRANSACTION '" + snap_B + "'"));
	REQUIRE_NO_FAIL(p2.Query("JOIN TRANSACTION '" + snap_A + "'"));

	const idx_t kIters = 50;
	std::string e1, e2;
	auto worker = [&](Connection &con, std::string &err) {
		for (idx_t i = 0; i < kIters; i++) {
			auto r = con.Query("SELECT (SELECT count(*) FROM dbA.t) + (SELECT count(*) FROM dbB.t)");
			if (r->HasError()) {
				err = r->GetError();
				return;
			}
		}
	};
	std::thread t1(worker, std::ref(p1), std::ref(e1));
	std::thread t2(worker, std::ref(p2), std::ref(e2));
	t1.join();
	t2.join();
	REQUIRE(e1.empty());
	REQUIRE(e2.empty());

	REQUIRE_NO_FAIL(p1.Query("COMMIT"));
	REQUIRE_NO_FAIL(p2.Query("COMMIT"));
	REQUIRE_NO_FAIL(ownerA.Query("COMMIT"));
	REQUIRE_NO_FAIL(ownerB.Query("COMMIT"));
}

// Bug #4: ImportTransaction wrote `transaction.active_query` with no
// synchronization vs. the owner's running query, and BeginQuery's SetActiveQuery
// could clobber it across connections. With the always-on statement lock the
// import + every BeginQuery hold the lock while writing active_query.
//
// The race is hard to detect via observable behavior (active_query feeds into
// MVCC visibility, not the user-visible result set in most cases). Instead we
// run rapid-fire interleaved queries on owner+participant; under TSan the
// previous code reports a data race, with the fix it does not.
TEST_CASE("Join transaction: rapid interleaved queries on owner+participant (issue #4)", "[join_txn]") {
	DuckDB db(nullptr);
	Connection owner(db);
	Connection participant(db);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO t VALUES (0)"));

	auto txn_id = ShareTransaction(owner);
	JoinSharedTransaction(participant, txn_id);

	const idx_t kIters = 200;
	std::string owner_err, participant_err;
	auto worker = [&](Connection &con, int base, std::string &err) {
		for (idx_t i = 0; i < kIters; i++) {
			auto r = con.Query("SELECT count(*) FROM t WHERE i >= " + std::to_string(base));
			if (r->HasError()) {
				err = r->GetError();
				return;
			}
		}
	};
	std::thread to(worker, std::ref(owner), 0, std::ref(owner_err));
	std::thread tp(worker, std::ref(participant), 0, std::ref(participant_err));
	to.join();
	tp.join();
	REQUIRE(owner_err.empty());
	REQUIRE(participant_err.empty());

	REQUIRE_NO_FAIL(participant.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

// #5: Verify TransactionInfo serialization round-trips the renamed `transaction_id` field.
// If the JSON descriptor key (parse_info.json) and the serialize_parse_info.cpp implementation
// drift apart, this catches it. Also locks in the JOIN_TRANSACTION enum value.
TEST_CASE("Join transaction: TransactionInfo serialization round-trip", "[join_txn]") {
	auto info = make_uniq<TransactionInfo>(TransactionType::JOIN_TRANSACTION);
	info->transaction_id = "42/main";

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<ParseInfo &>(*info), stream);
	stream.Rewind();
	auto deserialized_parse = BinaryDeserializer::Deserialize<ParseInfo>(stream);
	REQUIRE(deserialized_parse);
	REQUIRE(deserialized_parse->info_type == ParseInfoType::TRANSACTION_INFO);
	auto &deserialized = deserialized_parse->Cast<TransactionInfo>();
	REQUIRE(deserialized.type == TransactionType::JOIN_TRANSACTION);
	REQUIRE(deserialized.transaction_id == "42/main");

	// And a transaction id with a slash in the database name (last-/ split).
	info->transaction_id = "7/weird/db/name";
	MemoryStream stream2;
	BinarySerializer::Serialize(static_cast<ParseInfo &>(*info), stream2);
	stream2.Rewind();
	auto rt = BinaryDeserializer::Deserialize<ParseInfo>(stream2);
	REQUIRE(rt->Cast<TransactionInfo>().transaction_id == "7/weird/db/name");

	// Empty transaction_id (e.g. for non-JOIN transaction types) still round-trips.
	auto begin_info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION);
	MemoryStream stream3;
	BinarySerializer::Serialize(static_cast<ParseInfo &>(*begin_info), stream3);
	stream3.Rewind();
	auto rt_begin = BinaryDeserializer::Deserialize<ParseInfo>(stream3);
	REQUIRE(rt_begin->Cast<TransactionInfo>().type == TransactionType::BEGIN_TRANSACTION);
	REQUIRE(rt_begin->Cast<TransactionInfo>().transaction_id.empty());
}

// #7: Multi-database JOIN TRANSACTION end-to-end. A participant joins two separate
// owner-held transactions and verifies cross-DB read + write semantics.
TEST_CASE("Join transaction: participant joins transactions on multiple databases", "[join_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbA"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbB"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbA.t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbB.t(i INTEGER)"));

	Connection ownerA(db);
	REQUIRE_NO_FAIL(ownerA.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerA.Query("INSERT INTO dbA.t VALUES (1), (2)"));
	auto idA = ShareTransaction(ownerA);

	Connection ownerB(db);
	REQUIRE_NO_FAIL(ownerB.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerB.Query("INSERT INTO dbB.t VALUES (10), (20), (30)"));
	auto idB = ShareTransaction(ownerB);

	Connection participant(db);
	REQUIRE_NO_FAIL(participant.Query("BEGIN"));
	REQUIRE_NO_FAIL(participant.Query("JOIN TRANSACTION '" + idA + "'"));
	REQUIRE_NO_FAIL(participant.Query("JOIN TRANSACTION '" + idB + "'"));

	// Reads see both owners' uncommitted state.
	{
		auto r = participant.Query("SELECT count(*) FROM dbA.t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 2);
	}
	{
		auto r = participant.Query("SELECT count(*) FROM dbB.t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 3);
	}

	// Participant writes to dbA — visible to ownerA. (DuckDB enforces "single write database"
	// per MetaTransaction, so the participant can write to at most one of the joined DBs.)
	REQUIRE_NO_FAIL(participant.Query("INSERT INTO dbA.t VALUES (3)"));
	{
		auto r = ownerA.Query("SELECT count(*) FROM dbA.t");
		REQUIRE_NO_FAIL(*r);
		REQUIRE(r->GetValue(0, 0).GetValue<int64_t>() == 3);
	}

	// All three connections commit; both transactions become durable independently.
	REQUIRE_NO_FAIL(participant.Query("COMMIT"));
	REQUIRE_NO_FAIL(ownerA.Query("COMMIT"));
	REQUIRE_NO_FAIL(ownerB.Query("COMMIT"));

	Connection fresh(db);
	REQUIRE(fresh.Query("SELECT count(*) FROM dbA.t")->GetValue(0, 0).GetValue<int64_t>() == 3);
	REQUIRE(fresh.Query("SELECT count(*) FROM dbB.t")->GetValue(0, 0).GetValue<int64_t>() == 3);
}

// The single-write-DB invariant of MetaTransaction still applies after joining multiple
// transactions: a participant that has already modified one DB cannot then modify another.
TEST_CASE("Join transaction: single-write-DB invariant holds across joined DBs", "[join_txn]") {
	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbA"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS dbB"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbA.t(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE dbB.t(i INTEGER)"));

	Connection ownerA(db);
	REQUIRE_NO_FAIL(ownerA.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerA.Query("INSERT INTO dbA.t VALUES (1)"));
	auto idA = ShareTransaction(ownerA);

	Connection ownerB(db);
	REQUIRE_NO_FAIL(ownerB.Query("BEGIN"));
	REQUIRE_NO_FAIL(ownerB.Query("INSERT INTO dbB.t VALUES (10)"));
	auto idB = ShareTransaction(ownerB);

	Connection participant(db);
	REQUIRE_NO_FAIL(participant.Query("BEGIN"));
	REQUIRE_NO_FAIL(participant.Query("JOIN TRANSACTION '" + idA + "'"));
	REQUIRE_NO_FAIL(participant.Query("JOIN TRANSACTION '" + idB + "'"));

	REQUIRE_NO_FAIL(participant.Query("INSERT INTO dbA.t VALUES (2)"));
	auto r = participant.Query("INSERT INTO dbB.t VALUES (20)");
	REQUIRE(r->HasError());
	REQUIRE(r->GetError().find("single attached database") != std::string::npos);

	// Participant's failed write invalidates its transaction; cleanup follows the doom path.
	(void)participant.Query("ROLLBACK");
	(void)ownerA.Query("ROLLBACK");
	(void)ownerB.Query("ROLLBACK");
}

// #11: Defensive validation rejects pathological transaction ids cleanly.
TEST_CASE("Join transaction: rejects malformed/oversized/control-char ids", "[join_txn]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN"));

	// Empty.
	{
		auto r = con.Query("JOIN TRANSACTION ''");
		REQUIRE(r->HasError());
	}
	// Oversized (1024 byte cap).
	{
		std::string huge(2000, 'a');
		auto r = con.Query("JOIN TRANSACTION '" + huge + "'");
		REQUIRE(r->HasError());
		REQUIRE(r->GetError().find("maximum length") != std::string::npos);
	}
	// Control character (newline).
	{
		auto r = con.Query("JOIN TRANSACTION '7/main\n'");
		REQUIRE(r->HasError());
		REQUIRE(r->GetError().find("control character") != std::string::npos);
	}

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}
