#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Booleans and NULLs", "[sql]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// AND
	result = con.Query("SELECT 0 AND 0, 0 AND 1, 1 AND 0, 1 AND 1, NULL AND "
	                   "0, NULL AND 1, 0 AND NULL, 1 AND NULL, NULL AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {0}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 6, {0}));
	REQUIRE(CHECK_COLUMN(result, 7, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 8, {Value()}));

	// OR
	result = con.Query("SELECT 0 OR 0, 0 OR 1, 1 OR 0, 1 OR 1, NULL OR "
	                   "0, NULL OR 1, 0 OR NULL, 1 OR NULL, NULL OR NULL");

	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 7, {1}));
	REQUIRE(CHECK_COLUMN(result, 8, {Value()}));

	// NOT
	result = con.Query("SELECT NOT(0), NOT(1), NOT(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));

	// IS NULL
	result = con.Query("SELECT NULL IS NULL, NULL IS NOT NULL, 42 IS NULL, 42 IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));

	// Comparisions
	result = con.Query("SELECT NULL = NULL, NULL <> NULL, 42 = NULL, 42 <> NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (NULL, 21), (13, 22), (12, "
	          "NULL), (16, NULL)");

	result = con.Query("SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                   "GROUP BY b ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 0, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {28, Value(), 24}));
	REQUIRE(CHECK_COLUMN(result, 3, {12, Value(), 11}));
	REQUIRE(CHECK_COLUMN(result, 4, {16, Value(), 13}));

	// REQUIRE(duckdb_query(connection,
	//                      "SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM
	//                      test " "GROUP BY b ORDER BY b", &result) ==
	//                      DuckDBSuccess);
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC, 21, 22}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {2, 0, 2}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {28, NULL_NUMERIC, 24}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 3, {12, NULL_NUMERIC, 11}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 4, {16, NULL_NUMERIC, 13}));
	// duckdb_destroy_result(result);
}

// NOT
