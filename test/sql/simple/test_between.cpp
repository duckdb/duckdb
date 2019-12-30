#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test between statement", "[between]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test constant BETWEEN statement
	// simple between
	result = con.Query("SELECT 10 BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 9 BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// now with NULL values
	result = con.Query("SELECT 10 BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 30 BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT 10 BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 9 BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT NULL BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL BETWEEN NULL AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// between with table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	result = con.Query("SELECT i BETWEEN 1 AND 2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, true, false}));
	result = con.Query("SELECT i BETWEEN NULL AND 2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), false}));
	result = con.Query("SELECT i BETWEEN 2 AND NULL FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, Value(), Value()}));

	// between in WHERE clause
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 10 BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 9 BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 10 BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 30 BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 10 BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 9 BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN 10 AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN NULL AND 20");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN 10 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN NULL AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN 1 AND 2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN NULL AND 2");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN 2 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN -1 AND +1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 0 BETWEEN -1 AND +1");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN i-1 AND i+1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN i-1 AND 10");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN NULL AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN i-1 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN 0 AND i+1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i BETWEEN NULL AND i+1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 1 BETWEEN i-1 AND i+1");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN i-1 AND i+1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE NULL BETWEEN i-1 AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 1 BETWEEN i-1 AND 100");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE 1 BETWEEN 0 AND i-1");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}
