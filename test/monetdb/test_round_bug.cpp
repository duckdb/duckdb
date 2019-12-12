#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple round usage", "[round]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table test (col1 double);"));
	REQUIRE_NO_FAIL(con.Query("insert into test values (2.887);"));

	result = con.Query("select round(col1, -1) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {3.0}));
	result = con.Query("select round(col1, 0) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {3.0}));
	result = con.Query("select round(col1, 1) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {2.9}));
	result = con.Query("select round(col1, 2) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {2.89}));
	result = con.Query("select round(col1, 3) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {2.887}));
	result = con.Query("select round(col1, 4) from test;");
	REQUIRE(CHECK_COLUMN(result, 0, {2.887}));
}

TEST_CASE("MonetDB Test: round.Bug-3542.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	return;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_num_data (id integer, val numeric(18,10));"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_num_data VALUES (1, '-0.0');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_num_data VALUES (2, '-34338492.215397047');"));

	result = con.Query("SELECT * FROM test_num_data ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, -34338492.2153970470}));

	result = con.Query("SELECT t1.id, t2.id, t1.val * t2.val FROM test_num_data t1, test_num_data t2 ORDER BY 1, 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 0, 0, 1179132047626883.59686213585632020900}));

	result = con.Query(
	    "SELECT t1.id, t2.id, round(t1.val * t2.val, 30) FROM test_num_data t1, test_num_data t2 ORDER BY 1, 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 0, 0, 1179132047626883.596862135856320209000000000000}));
}
