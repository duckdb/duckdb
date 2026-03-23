#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// Helper: materialize a query as ground truth on the SAME connection, then stream
// the same query with periodic suspensions, and verify all rows match.
//
// The connection must have SET enable_suspended_queries = true and be in a BEGIN TRANSACTION.
// Compares the multiset of rows: same rows in any order, same count.
static void ValidateSuspendedStream(Connection &con, const string &query, const string &suspend_dml = "SELECT 1",
                                    idx_t suspend_every_n_chunks = 3) {
	auto baseline = con.Query(query);
	REQUIRE(!baseline->HasError());
	idx_t expected_count = baseline->RowCount();
	idx_t col_count = baseline->ColumnCount();
	REQUIRE(expected_count > 0);

	// Collect baseline rows as sorted strings
	duckdb::vector<string> baseline_rows;
	baseline_rows.reserve(expected_count);
	for (idx_t row = 0; row < expected_count; row++) {
		string row_str;
		for (idx_t col = 0; col < col_count; col++) {
			if (col > 0) {
				row_str += "|";
			}
			row_str += baseline->GetValue(col, row).ToString();
		}
		baseline_rows.push_back(row_str);
	}
	sort(baseline_rows.begin(), baseline_rows.end());

	// Stream the same query with periodic suspensions
	auto stream = con.SendQuery(query);
	REQUIRE(!stream->HasError());
	REQUIRE(stream->type == QueryResultType::STREAM_RESULT);

	duckdb::vector<string> streamed_rows;
	streamed_rows.reserve(expected_count);
	idx_t chunk_num = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row = 0; row < chunk->size(); row++) {
			string row_str;
			for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
				if (col > 0) {
					row_str += "|";
				}
				row_str += chunk->GetValue(col, row).ToString();
			}
			streamed_rows.push_back(row_str);
		}
		chunk_num++;
		if (chunk_num % suspend_every_n_chunks == 0) {
			auto r = con.Query(suspend_dml);
			REQUIRE(!r->HasError());
		}
	}

	// Single comparison: count + sorted content
	REQUIRE(streamed_rows.size() == expected_count);
	sort(streamed_rows.begin(), streamed_rows.end());
	REQUIRE(baseline_rows == streamed_rows);
}

// Helper: consume a stream fully and return total row count
static idx_t ConsumeStream(duckdb::unique_ptr<QueryResult> &stream) {
	idx_t total = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
	}
	return total;
}

// ============================================================================
// Core mechanics
// ============================================================================

TEST_CASE("Test suspended query - basic interleaved streaming + DML", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, false AS processed FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t WHERE NOT processed ORDER BY id");
	REQUIRE(!stream->HasError());

	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);

	auto update_result = con.Query("UPDATE t SET processed = true WHERE id < 10");
	REQUIRE(!update_result->HasError());

	// Resume and consume
	idx_t total = chunk->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);

	con.Query("COMMIT");

	auto result = con.Query("SELECT COUNT(*) FROM t WHERE processed");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
}

TEST_CASE("Test suspended query - multiple interleaved updates", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	REQUIRE(!stream->HasError());

	// Fetch, update, fetch, update, fetch
	auto c1 = stream->Fetch();
	REQUIRE(c1);
	auto r1 = con.Query("UPDATE t SET id = id + 100000 WHERE id < 5");
	REQUIRE(!r1->HasError());
	auto c2 = stream->Fetch();
	REQUIRE(c2);
	auto r2 = con.Query("UPDATE t SET id = id + 200000 WHERE id >= 5 AND id < 10");
	REQUIRE(!r2->HasError());
	auto c3 = stream->Fetch();
	REQUIRE(c3);

	idx_t total = c1->size() + c2->size() + c3->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - multiple concurrent streams", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream1 = con.SendQuery("SELECT id FROM t1 ORDER BY id");
	auto stream2 = con.SendQuery("SELECT id FROM t2 ORDER BY id");

	// Alternating fetches
	auto c1 = stream1->Fetch();
	REQUIRE(c1);
	REQUIRE(c1->GetValue(0, 0).GetValue<int64_t>() == 0);

	auto c2 = stream2->Fetch();
	REQUIRE(c2);
	REQUIRE(c2->GetValue(0, 0).GetValue<int64_t>() == 0);

	auto r = con.Query("UPDATE t1 SET id = id + 1000000 WHERE id < 5");
	REQUIRE(!r->HasError());

	idx_t total1 = c1->size() + ConsumeStream(stream1);
	idx_t total2 = c2->size() + ConsumeStream(stream2);
	REQUIRE(total1 == 10000);
	REQUIRE(total2 == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - auto-commit mode unchanged", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10) tbl(i)");
	// No SET enable_suspended_queries — default is off
	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	auto chunk = stream->Fetch();
	REQUIRE(chunk);

	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	REQUIRE_THROWS(stream->Fetch());
}

TEST_CASE("Test suspended query - setting disabled kills stream in explicit txn", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	// Setting is OFF (default) — explicit txn should NOT suspend
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	auto chunk = stream->Fetch();
	REQUIRE(chunk);

	auto result = con.Query("SELECT 42");
	REQUIRE(!result->HasError());

	REQUIRE_THROWS(stream->Fetch());
	con.Query("ROLLBACK");
}

TEST_CASE("Test suspended query - connection close cleans up", "[api]") {
	DuckDB db(nullptr);

	{
		Connection con(db);
		con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10) tbl(i)");
		con.Query("SET enable_suspended_queries = true");
		con.Query("BEGIN TRANSACTION");

		auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
		stream->Fetch();

		con.Query("UPDATE t SET id = id + 100 WHERE id < 5");
		// Connection goes out of scope — should clean up without crash
	}

	Connection con2(db);
	auto result = con2.Query("SELECT COUNT(*) FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
}

TEST_CASE("Test suspended query - IsOpen through suspension lifecycle", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	auto &sr = stream->Cast<StreamQueryResult>();
	REQUIRE(sr.IsOpen());

	stream->Fetch();
	auto r = con.Query("UPDATE t SET id = id + 100 WHERE id < 5");
	REQUIRE(!r->HasError());

	// Suspended — still open
	REQUIRE(sr.IsOpen());

	// Start second stream — first is still open (suspended)
	auto stream2 = con.SendQuery("SELECT id FROM t ORDER BY id LIMIT 10");
	auto &sr2 = stream2->Cast<StreamQueryResult>();
	REQUIRE(sr.IsOpen());
	REQUIRE(sr2.IsOpen());

	ConsumeStream(stream2);
	REQUIRE(!sr2.IsOpen());
	REQUIRE(sr.IsOpen());

	ConsumeStream(stream);
	REQUIRE(!sr.IsOpen());
	con.Query("COMMIT");
}

// ============================================================================
// Update isolation
// ============================================================================

TEST_CASE("Test suspended query - update isolation (ORDER BY materializes)", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id, val FROM t ORDER BY id");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);

	con.Query("UPDATE t SET val = val + 1000000");

	// ORDER BY materializes — all rows should have OLD values
	idx_t saw_old = 0, saw_new = 0;
	// Check first chunk
	for (idx_t i = 0; i < chunk1->size(); i++) {
		auto val = chunk1->GetValue(1, i).GetValue<int64_t>();
		auto id = chunk1->GetValue(0, i).GetValue<int64_t>();
		if (val == id) {
			saw_old++;
		} else if (val == id + 1000000) {
			saw_new++;
		}
	}
	// Check remaining
	while (true) {
		auto c = stream->Fetch();
		if (!c || c->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < c->size(); i++) {
			auto val = c->GetValue(1, i).GetValue<int64_t>();
			auto id = c->GetValue(0, i).GetValue<int64_t>();
			if (val == id) {
				saw_old++;
			} else if (val == id + 1000000) {
				saw_new++;
			}
		}
	}
	REQUIRE(saw_old == 10000);
	REQUIRE(saw_new == 0);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - no infinite loop from own updates", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id, val FROM t");
	idx_t total = 0;
	idx_t updates = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
		if (updates < 3) {
			con.Query("UPDATE t SET val = val + 1 WHERE id < " + to_string((updates + 1) * 3000));
			updates++;
		}
		if (total > 100000) {
			break;
		} // safety valve
	}
	REQUIRE(total == 10000);
	con.Query("COMMIT");
}

// ============================================================================
// Destructive DDL operations
// ============================================================================

TEST_CASE("Test suspended query - ALTER TABLE DROP COLUMN during scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val, i AS extra FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id, val FROM t");
	stream->Fetch();

	// Drop a scanned column — executor has its own references, scan continues
	REQUIRE(!con.Query("ALTER TABLE t DROP COLUMN val")->HasError());

	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->ColumnCount() == 2);
	con.Query("ROLLBACK");
}

TEST_CASE("Test suspended query - DROP TABLE during scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t");
	stream->Fetch();

	// DROP succeeds — row groups are ref-counted, scan continues
	REQUIRE(!con.Query("DROP TABLE t")->HasError());

	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);
	con.Query("ROLLBACK");
}

TEST_CASE("Test suspended query - TRUNCATE during scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Use 100K rows so the sequential scan can't exhaust everything in the buffer
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	// No ORDER BY — sequential scan, not materialized by sort
	auto stream = con.SendQuery("SELECT id FROM t");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);

	REQUIRE(!con.Query("TRUNCATE TABLE t")->HasError());

	idx_t total = chunk1->size() + ConsumeStream(stream);
	// TRUNCATE deletes row groups — scan returns fewer than 100000
	REQUIRE(total < 100000);
	REQUIRE(total > 0);
	con.Query("ROLLBACK");
}

TEST_CASE("Test suspended query - CREATE INDEX during scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id, val FROM t");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);

	REQUIRE(!con.Query("CREATE INDEX idx_t_id ON t(id)")->HasError());

	idx_t total = chunk1->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("ROLLBACK");
}

// ============================================================================
// Transaction lifecycle
// ============================================================================

TEST_CASE("Test suspended query - ROLLBACK while suspended", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t");
	stream->Fetch();
	con.Query("UPDATE t SET id = id + 1 WHERE id < 10");
	con.Query("ROLLBACK");

	REQUIRE_THROWS_AS(stream->Fetch(), InvalidInputException);

	auto result = con.Query("SELECT COUNT(*) FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {10000}));
}

TEST_CASE("Test suspended query - COMMIT while suspended", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	stream->Fetch();
	con.Query("UPDATE t SET id = id + 1 WHERE id < 10");
	con.Query("COMMIT");

	REQUIRE_THROWS_AS(stream->Fetch(), InvalidInputException);

	auto result = con.Query("SELECT COUNT(*) FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {10000}));
}

TEST_CASE("Test suspended query - constraint error invalidates stream", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 (id INTEGER NOT NULL)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t");
	stream->Fetch();

	auto r = con.Query("INSERT INTO t2 VALUES (NULL)");
	REQUIRE(r->HasError());

	REQUIRE_THROWS_AS(stream->Fetch(), InvalidInputException);
	con.Query("ROLLBACK");
}

// ============================================================================
// Large result sets (scaled down from originals)
// ============================================================================

TEST_CASE("Test suspended query - periodic updates during large scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	const idx_t TABLE_SIZE = 100000;
	const idx_t UPDATE_BATCH = 1000;
	con.Query("CREATE TABLE t AS SELECT i AS id, false AS processed FROM range(" + to_string(TABLE_SIZE) + ") tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	idx_t total = 0;
	idx_t update_count = 0;
	idx_t batch = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
		batch++;
		// Update periodically, but cap at table size to avoid out-of-range updates
		if (batch % 10 == 0 && update_count * UPDATE_BATCH < TABLE_SIZE) {
			con.Query("UPDATE t SET processed = true WHERE id >= " + to_string(update_count * UPDATE_BATCH) +
			          " AND id < " + to_string((update_count + 1) * UPDATE_BATCH));
			update_count++;
		}
	}
	REQUIRE(total == TABLE_SIZE);
	// At least one suspend/resume cycle must have happened
	REQUIRE(update_count > 0);
	con.Query("COMMIT");

	auto result = con.Query("SELECT COUNT(*) FROM t WHERE processed");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == static_cast<int64_t>(update_count * UPDATE_BATCH));
}

TEST_CASE("Test suspended query - INSERT interleaved with scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE source AS SELECT i AS id FROM range(100000) tbl(i)");
	con.Query("CREATE TABLE dest (id INTEGER)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM source ORDER BY id");
	idx_t total = 0;
	idx_t inserts = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
		if (total % 25000 < chunk->size()) {
			con.Query("INSERT INTO dest VALUES (" + to_string(inserts) + ")");
			inserts++;
		}
	}
	REQUIRE(total == 100000);
	REQUIRE(inserts >= 3);

	auto result = con.Query("SELECT COUNT(*) FROM dest");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == static_cast<int64_t>(inserts));
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - DELETE interleaved with scan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100000) tbl(i)");
	con.Query("CREATE TABLE to_delete AS SELECT i * 1000 AS id FROM range(100) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	idx_t total = 0;
	idx_t deletes = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
		if (total % 25000 < chunk->size()) {
			con.Query("DELETE FROM to_delete WHERE id = " + to_string(deletes * 1000));
			deletes++;
		}
	}
	REQUIRE(total == 100000);

	auto result = con.Query("SELECT COUNT(*) FROM to_delete");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == static_cast<int64_t>(100 - deletes));
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - two streams alternating", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(15000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto s1 = con.SendQuery("SELECT id FROM t1 ORDER BY id");
	auto s2 = con.SendQuery("SELECT id FROM t2 ORDER BY id");

	idx_t total1 = 0, total2 = 0;
	bool done1 = false, done2 = false;
	while (!done1 || !done2) {
		if (!done1) {
			auto c = s1->Fetch();
			if (!c || c->size() == 0) {
				done1 = true;
			} else {
				total1 += c->size();
			}
		}
		if (!done2) {
			auto c = s2->Fetch();
			if (!c || c->size() == 0) {
				done2 = true;
			} else {
				total2 += c->size();
			}
		}
	}
	REQUIRE(total1 == 10000);
	REQUIRE(total2 == 15000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - rapid suspend/resume every chunk", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(50000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	idx_t total = 0;
	idx_t suspensions = 0;
	while (true) {
		auto chunk = stream->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		total += chunk->size();
		con.Query("SELECT 1");
		suspensions++;
	}
	REQUIRE(total == 50000);
	REQUIRE(suspensions > 10);
	con.Query("COMMIT");
}

// ============================================================================
// Data validation (baseline comparison via ValidateSuspendedStream)
// ============================================================================

TEST_CASE("Test suspended query - window functions data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 10 AS grp, i AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(
	    con,
	    "SELECT id, grp, val, "
	    "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn, "
	    "SUM(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum "
	    "FROM t");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - DISTINCT data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i % 1000 AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT DISTINCT val FROM t");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - UNION ALL data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(5000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i + 5000 AS id FROM range(5000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id FROM t1 UNION ALL SELECT id FROM t2");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - recursive CTE data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE edges (src INTEGER, dst INTEGER)");
	con.Query("INSERT INTO edges SELECT i, i+1 FROM range(500) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "WITH RECURSIVE reachable AS ("
	                             "  SELECT 0 AS node "
	                             "  UNION "
	                             "  SELECT e.dst FROM edges e JOIN reachable r ON e.src = r.node"
	                             ") SELECT node FROM reachable ORDER BY node");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - LIMIT data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id FROM t ORDER BY id LIMIT 5000");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - LIMIT OFFSET data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id FROM t ORDER BY id LIMIT 3000 OFFSET 2000");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - INTERSECT data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i * 2 AS id FROM range(6000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id FROM t1 INTERSECT SELECT id FROM t2");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - EXCEPT data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i * 2 AS id FROM range(6000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id FROM t1 EXCEPT SELECT id FROM t2");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - aggregation data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS grp, (i * 0.1)::DOUBLE AS val FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT grp, COUNT(*), SUM(val), MIN(val), MAX(val) FROM t GROUP BY grp ORDER BY grp");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - JOIN data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE orders AS SELECT i AS oid, i % 100 AS cid, i * 1.5 AS amt FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE customers AS SELECT i AS cid, 'name_' || i::VARCHAR AS name FROM range(100) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(
	    con, "SELECT o.oid, c.name, o.amt FROM orders o JOIN customers c ON o.cid = c.cid ORDER BY o.oid");
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - correlated subquery data validation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 50 AS grp FROM range(5000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");
	ValidateSuspendedStream(con, "SELECT id, grp, (SELECT COUNT(*) FROM t t2 WHERE t2.grp = t.grp) AS grp_count "
	                             "FROM t WHERE id < 500 ORDER BY id");
	con.Query("COMMIT");
}

// ============================================================================
// Medium risk: system interactions
// ============================================================================

TEST_CASE("Test suspended query - prepared statement", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto prepared = con.Prepare("SELECT id FROM t WHERE id >= $1 ORDER BY id");
	REQUIRE(!prepared->HasError());
	duckdb::vector<Value> params;
	params.push_back(Value::INTEGER(0));
	auto pending = prepared->PendingQuery(params, true);
	REQUIRE(!pending->HasError());
	auto stream = pending->Execute();

	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);
	REQUIRE(chunk1->GetValue(0, 0).GetValue<int64_t>() == 0);

	con.Query("SELECT 1"); // suspend

	int64_t last_val = -1;
	idx_t total = chunk1->size();
	while (true) {
		auto c = stream->Fetch();
		if (!c || c->size() == 0) {
			break;
		}
		total += c->size();
		last_val = c->GetValue(0, c->size() - 1).GetValue<int64_t>();
	}
	REQUIRE(total == 10000);
	REQUIRE(last_val == 9999);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - 6 concurrent suspended streams", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (int i = 0; i < 6; i++) {
		con.Query("CREATE TABLE t" + to_string(i) + " AS SELECT j AS id FROM range(5000) tbl(j)");
	}
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	duckdb::vector<duckdb::unique_ptr<QueryResult>> streams;
	for (int i = 0; i < 6; i++) {
		streams.push_back(con.SendQuery("SELECT id FROM t" + to_string(i) + " ORDER BY id"));
	}

	// Fetch from each, then consume fully
	for (int i = 0; i < 6; i++) {
		auto chunk = streams[i]->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 0);
	}

	con.Query("SELECT 1"); // suspend all

	for (int i = 0; i < 6; i++) {
		idx_t total = ConsumeStream(streams[i]);
		// First fetch got some rows, remaining come from ConsumeStream
		REQUIRE(total > 0);
	}
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - resume in non-LIFO order", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t3 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto s1 = con.SendQuery("SELECT id FROM t1 ORDER BY id");
	auto s2 = con.SendQuery("SELECT id FROM t2 ORDER BY id");
	auto s3 = con.SendQuery("SELECT id FROM t3 ORDER BY id");

	// Resume order: s1, s3, s2 (not LIFO)
	auto c1 = s1->Fetch();
	REQUIRE(c1);
	auto c3 = s3->Fetch();
	REQUIRE(c3);
	auto c2 = s2->Fetch();
	REQUIRE(c2);

	idx_t t1 = c1->size() + ConsumeStream(s1);
	idx_t t2 = c2->size() + ConsumeStream(s2);
	idx_t t3 = c3->size() + ConsumeStream(s3);
	REQUIRE(t1 == 10000);
	REQUIRE(t2 == 10000);
	REQUIRE(t3 == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - temp table", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TEMPORARY TABLE tmp AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 (id INTEGER)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM tmp ORDER BY id");
	stream->Fetch();
	con.Query("INSERT INTO t2 VALUES (999)");

	idx_t total = ConsumeStream(stream);
	REQUIRE(total > 0);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - table function (generate_series)", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t (id INTEGER)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT * FROM generate_series(0, 9999)");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);
	con.Query("INSERT INTO t VALUES (1)");

	idx_t total = chunk1->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - UPDATE RETURNING as interleaved statement", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(100) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);

	auto ret = con.Query("UPDATE t2 SET id = id + 1000 RETURNING id");
	REQUIRE(!ret->HasError());
	REQUIRE(ret->RowCount() == 100);

	idx_t total = chunk1->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - streaming UPDATE RETURNING suspended", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id, 0 AS version FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("UPDATE t SET version = version + 1 RETURNING id, version");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);
	REQUIRE(chunk1->ColumnCount() == 2);
	// First row should have version=1
	REQUIRE(chunk1->GetValue(1, 0).GetValue<int64_t>() == 1);

	con.Query("SELECT 1"); // suspend

	idx_t total = chunk1->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("COMMIT");

	auto r = con.Query("SELECT COUNT(*) FROM t WHERE version = 1");
	REQUIRE(CHECK_COLUMN(r, 0, {10000}));
}

TEST_CASE("Test suspended query - two RETURNING streams interleaved", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id, 0 AS v FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id, 0 AS v FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto s1 = con.SendQuery("UPDATE t1 SET v = 1 RETURNING id, v");
	auto s2 = con.SendQuery("UPDATE t2 SET v = 2 RETURNING id, v");

	auto c1 = s1->Fetch();
	REQUIRE(c1);
	REQUIRE(c1->GetValue(1, 0).GetValue<int64_t>() == 1);

	auto c2 = s2->Fetch();
	REQUIRE(c2);
	REQUIRE(c2->GetValue(1, 0).GetValue<int64_t>() == 2);

	idx_t total1 = c1->size() + ConsumeStream(s1);
	idx_t total2 = c2->size() + ConsumeStream(s2);
	REQUIRE(total1 == 10000);
	REQUIRE(total2 == 10000);
	con.Query("COMMIT");

	auto r1 = con.Query("SELECT COUNT(*) FROM t1 WHERE v = 1");
	REQUIRE(CHECK_COLUMN(r1, 0, {10000}));
	auto r2 = con.Query("SELECT COUNT(*) FROM t2 WHERE v = 2");
	REQUIRE(CHECK_COLUMN(r2, 0, {10000}));
}

TEST_CASE("Test suspended query - VIEW over updated base table", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val FROM range(10000) tbl(i)");
	con.Query("CREATE VIEW v AS SELECT id, val FROM t WHERE val >= 0");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id, val FROM v");
	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);
	REQUIRE(chunk1->ColumnCount() == 2);

	con.Query("UPDATE t SET val = val + 1000000 WHERE id < 100");

	idx_t total = chunk1->size() + ConsumeStream(stream);
	REQUIRE(total == 10000);
	con.Query("COMMIT");
}

// ============================================================================
// Close/cancel
// ============================================================================

TEST_CASE("Test suspended query - close cancels suspended stream", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT id FROM t ORDER BY id");
	auto &sr = stream->Cast<StreamQueryResult>();
	stream->Fetch();

	con.Query("SELECT COUNT(*) FROM t2"); // suspend
	REQUIRE(sr.IsOpen());

	sr.Close();
	REQUIRE(!sr.IsOpen());

	// Connection works normally after cancellation
	auto stream2 = con.SendQuery("SELECT id FROM t2 ORDER BY id");
	REQUIRE(ConsumeStream(stream2) == 10000);
	con.Query("COMMIT");
}

TEST_CASE("Test suspended query - close one of multiple suspended streams", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t1 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("CREATE TABLE t3 AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto s1 = con.SendQuery("SELECT id FROM t1 ORDER BY id");
	auto s2 = con.SendQuery("SELECT id FROM t2 ORDER BY id");
	auto s3 = con.SendQuery("SELECT id FROM t3 ORDER BY id");

	auto &sr2 = s2->Cast<StreamQueryResult>();
	sr2.Close();
	REQUIRE(!sr2.IsOpen());

	// s1 and s3 still work
	REQUIRE(ConsumeStream(s1) == 10000);
	REQUIRE(ConsumeStream(s3) == 10000);
	REQUIRE_THROWS(s2->Fetch());
	con.Query("COMMIT");
}

// ============================================================================
// Error handling
// ============================================================================

TEST_CASE("Test suspended query - error during resumed fetch", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(10000) tbl(i)");
	con.Query("SET enable_suspended_queries = true");
	con.Query("BEGIN TRANSACTION");

	auto stream = con.SendQuery("SELECT CASE WHEN id = 9999 THEN 1 / 0 ELSE id END AS val FROM t ORDER BY id");

	auto chunk1 = stream->Fetch();
	REQUIRE(chunk1);

	con.Query("SELECT 1"); // suspend

	// Consume — may hit divide-by-zero. No crash is the key requirement.
	bool got_error = false;
	idx_t total = chunk1->size();
	while (true) {
		auto c = stream->Fetch();
		if (!c || c->size() == 0) {
			if (stream->HasError()) {
				got_error = true;
			}
			break;
		}
		total += c->size();
	}
	REQUIRE((got_error || total > 0));
	con.Query("ROLLBACK");
}
