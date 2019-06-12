#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test joins under setops", "[setops][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(b INTEGER);"));
	for (size_t i = 0; i < 1024; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(i) + ")"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(i) + ")"));
	}
	auto result =
	    con.Query("(SELECT * FROM test, test2 WHERE a=b) UNION (SELECT * FROM test,test2 WHERE a=b) ORDER BY 1");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->collection.count == 1024);
}

TEST_CASE("Test joins under setops with CTEs", "[setops][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(b INTEGER);"));
	for (size_t i = 0; i < 1024; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(i) + ")"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(i) + ")"));
	}
	result = con.Query("WITH test_cte AS ((SELECT * FROM test, test2 WHERE a=b) UNION (SELECT * FROM test,test2 WHERE "
	                   "a=b)) SELECT SUM(ta.a) FROM test_cte ta, test_cte tb WHERE ta.a=tb.a");
	REQUIRE(CHECK_COLUMN(result, 0, {523776}));
}

TEST_CASE("Test joins under setops with CTEs and aggregations", "[setops][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(b INTEGER);"));
	for (size_t i = 0; i < 1024; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(i) + ")"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(i) + ")"));
	}
	result = con.Query("WITH test_cte AS ((SELECT * FROM test, test2 WHERE a=b) UNION (SELECT * FROM test,test2 WHERE "
	                   "a=b)), results AS (SELECT SUM(ta.a) AS sum_a FROM test_cte ta, test_cte tb WHERE ta.a=tb.a) "
	                   "SELECT * FROM (SELECT * FROM results GROUP BY sum_a UNION SELECT * FROM results GROUP BY sum_a "
	                   "UNION SELECT * FROM results GROUP BY sum_a UNION SELECT * FROM results GROUP BY sum_a) AS t");
	REQUIRE(CHECK_COLUMN(result, 0, {523776}));
}
