#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar aggregates with many different types", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// count
	result = con.Query("SELECT COUNT(), COUNT(1), COUNT(*), COUNT(NULL), COUNT('hello'), COUNT(DATE '1992-02-02')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));

	REQUIRE_FAIL(con.Query("SELECT COUNT(1, 2)"));
	// sum
	result = con.Query("SELECT SUM(1), SUM(NULL), SUM(33.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));

	REQUIRE_FAIL(con.Query("SELECT SUM(True)"));
	REQUIRE_FAIL(con.Query("SELECT SUM('hello')"));
	REQUIRE_FAIL(con.Query("SELECT SUM(DATE '1992-02-02')"));
	REQUIRE_FAIL(con.Query("SELECT SUM()"));
	REQUIRE_FAIL(con.Query("SELECT SUM(1, 2)"));
	// min
	result = con.Query("SELECT MIN(1), MIN(NULL), MIN(33.3), MIN('hello'), MIN(True), MIN(DATE '1992-02-02'), "
	                   "MIN(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT MIN()"));
	REQUIRE_FAIL(con.Query("SELECT MIN(1, 2)"));
	// max
	result = con.Query("SELECT MAX(1), MAX(NULL), MAX(33.3), MAX('hello'), MAX(True), MAX(DATE '1992-02-02'), "
	                   "MAX(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT MAX()"));
	REQUIRE_FAIL(con.Query("SELECT MAX(1, 2)"));
	// first
	result = con.Query("SELECT FIRST(1), FIRST(NULL), FIRST(33.3), FIRST('hello'), FIRST(True), FIRST(DATE "
	                   "'1992-02-02'), FIRST(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT FIRST()"));
	REQUIRE_FAIL(con.Query("SELECT FIRST(1, 2)"));

	// avg
	result = con.Query("SELECT AVG(1), AVG(NULL), AVG(33.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));

	REQUIRE_FAIL(con.Query("SELECT AVG(True)"));
	REQUIRE_FAIL(con.Query("SELECT AVG('hello')"));
	REQUIRE_FAIL(con.Query("SELECT AVG(DATE '1992-02-02')"));
	REQUIRE_FAIL(con.Query("SELECT AVG()"));
	REQUIRE_FAIL(con.Query("SELECT AVG(1, 2)"));

	// string agg
	result = con.Query("SELECT STRING_AGG('hello', ' '), STRING_AGG('hello', NULL), STRING_AGG(NULL, ' '), "
	                   "STRING_AGG(NULL, NULL), STRING_AGG('', '')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {""}));

	REQUIRE_FAIL(con.Query("SELECT STRING_AGG()"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG('hello')"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(1, 2, 3)"));
}

TEST_CASE("Test aggregates with many different types", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// strings
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, g INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 0), ('world', 1), (NULL, 0), ('r', 1)"));

	// simple aggregates only
	result = con.Query("SELECT COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"world"}));
	// simple aggr with only NULL values
	result = con.Query("SELECT COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings WHERE s IS NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	// add string_agg
	result = con.Query(
	    "SELECT STRING_AGG(s, ' '), STRING_AGG(s, ''), STRING_AGG('', ''), STRING_AGG('hello', ' ') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello world r"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"helloworldr"}));
	REQUIRE(CHECK_COLUMN(result, 2, {""}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello hello hello hello"}));

	// more complex agg (groups)
	result = con.Query(
	    "SELECT g, COUNT(*), COUNT(s), MIN(s), MAX(s), STRING_AGG(s, ' ') FROM strings GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello", "r"}));
	REQUIRE(CHECK_COLUMN(result, 4, {"hello", "world"}));
	REQUIRE(CHECK_COLUMN(result, 5, {"hello", "world r"}));
	// empty group
	result = con.Query("SELECT g, COUNT(*), COUNT(s), MIN(s), MAX(s), STRING_AGG(s, ' ') FROM strings WHERE s IS NULL "
	                   "OR s <> 'hello' GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), "r"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), "world"}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), "world r"}));

	// unsupported aggregates
	REQUIRE_FAIL(con.Query("SELECT SUM(s) FROM strings GROUP BY g ORDER BY g"));
	REQUIRE_FAIL(con.Query("SELECT AVG(s) FROM strings GROUP BY g ORDER BY g"));

	// booleans
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE booleans(b BOOLEAN, g INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO booleans VALUES (false, 0), (true, 1), (NULL, 0), (false, 1)"));

	// simple agg (no grouping)
	result = con.Query("SELECT COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {false}));
	REQUIRE(CHECK_COLUMN(result, 3, {true}));
	// simple agg with only null values
	result = con.Query("SELECT COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans WHERE b IS NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	// more complex agg (groups)
	result = con.Query("SELECT g, COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {false, true}));
	// more complex agg with empty groups
	result = con.Query(
	    "SELECT g, COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans WHERE b IS NULL OR b=true GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), true}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), true}));

	// unsupported aggregates
	REQUIRE_FAIL(con.Query("SELECT SUM(b) FROM booleans GROUP BY g ORDER BY g"));
	REQUIRE_FAIL(con.Query("SELECT AVG(b) FROM booleans GROUP BY g ORDER BY g"));

	// integers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, g INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (12, 0), (22, 1), (NULL, 0), (14, 1)"));

	// simple agg (no grouping)
	result = con.Query("SELECT COUNT(*), COUNT(i), MIN(i), MAX(i), SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {12}));
	REQUIRE(CHECK_COLUMN(result, 3, {22}));
	REQUIRE(CHECK_COLUMN(result, 4, {48}));
	// simple agg with only null values
	result = con.Query("SELECT COUNT(*), COUNT(i), MIN(i), MAX(i), SUM(i) FROM INTEGERS WHERE i IS NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	// more complex agg (groups)
	result = con.Query("SELECT g, COUNT(*), COUNT(i), MIN(i), MAX(i), SUM(i) FROM integers GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {12, 14}));
	REQUIRE(CHECK_COLUMN(result, 4, {12, 22}));
	REQUIRE(CHECK_COLUMN(result, 5, {12, 36}));
	// more complex agg with empty groups
	result = con.Query("SELECT g, COUNT(*), COUNT(i), MIN(i), MAX(i), SUM(i) FROM integers WHERE i IS NULL OR i > 15 "
	                   "GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), 22}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), 22}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), 22}));
}
