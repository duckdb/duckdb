#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test LIMIT keyword", "[limit]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22)"));

	// constant limit
	result = con.Query("SELECT a FROM test LIMIT 1");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));

	// decimal limit
	result = con.Query("SELECT a FROM test LIMIT 1.5");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));

	// LIMIT with operation
	result = con.Query("SELECT a FROM test LIMIT 2-1");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));

	// LIMIT with non-scalar should fail
	REQUIRE_FAIL(con.Query("SELECT a FROM test LIMIT a"));
	// LIMIT with non-scalar operation should also fail
	REQUIRE_FAIL(con.Query("SELECT a FROM test LIMIT a+1"));

	// aggregate in limit
	REQUIRE_FAIL(con.Query("SELECT a FROM test LIMIT SUM(42)"));
	// window function in limit
	REQUIRE_FAIL(con.Query("SELECT a FROM test LIMIT row_number() OVER ()"));
	// subquery in limit
	REQUIRE_FAIL(con.Query("SELECT a FROM test LIMIT (SELECT MIN(a) FROM test)"));
}

TEST_CASE("LIMIT Bug #321 Crazy Result", "[limit]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a STRING);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('Hello World')"));

	auto prep = con.Prepare("SELECT * FROM test LIMIT 3");
	vector<Value> params;
	params.clear();
	result = prep->Execute(params);
	REQUIRE(CHECK_COLUMN(result, 0, {"Hello World"}));
}
