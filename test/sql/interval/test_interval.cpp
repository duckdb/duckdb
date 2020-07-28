#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/interval.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test various ops involving intervals", "[interval]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	// create table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE interval (t INTERVAL);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO interval VALUES (INTERVAL '20' DAY), (INTERVAL '1' YEAR), (INTERVAL '1' MONTH);"));

	// count distinct
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// update
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE interval SET t=INTERVAL '1' MONTH WHERE t=INTERVAL '20' DAY;"));
	// now we only have two distinct values in con
	result = con.Query("SELECT * FROM interval ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// in con2 we still have 3
	result = con2.Query("SELECT * FROM interval ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 20, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con2.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// rollback
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// after the rollback we are back to 3
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now commit it
	REQUIRE_NO_FAIL(con.Query("UPDATE interval SET t=INTERVAL '1' MONTH WHERE t=INTERVAL '20' DAY;"));
	result = con.Query("SELECT t, COUNT(*) FROM interval GROUP BY t ORDER BY 2 DESC");
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1}));
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con2.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 USING (t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 ON (i1.t <> i2.t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0),  Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTERVAL(12, 0, 0), Value::INTERVAL(12, 0, 0), Value::INTERVAL(1, 0, 0),  Value::INTERVAL(1, 0, 0)}));
	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 ON (i1.t > i2.t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(12, 0, 0), Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0)}));

	result = con.Query("SELECT t, row_number() OVER (PARTITION BY t ORDER BY t) FROM interval ORDER BY 1, 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1}));
}
