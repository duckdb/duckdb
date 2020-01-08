#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test value list in selection", "[valuelist]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// value list can be a top-level statement
	result = con.Query("(VALUES (1, 3), (2, 4));");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 4}));

	// nulls first and then integers
	result = con.Query("SELECT * FROM (VALUES (NULL, NULL), (3, 4), (3, 7)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4, 7}));

	// standard value list
	result = con.Query("SELECT * FROM (VALUES (1, 2, 3), (1, 2, 3)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3}));

	// value list with expressions
	result = con.Query("SELECT * FROM (VALUES (1 + 1, 2, 3), (1 + 3, 2, 3)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3}));

	// value list with subqueries
	result = con.Query("SELECT * FROM (VALUES ((SELECT 42), 2, 3), (1 + 3,2,3)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3}));

	// value list in insert
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 2), (3, 4);"));

	// value list with more complicated subqueries
	result =
	    con.Query("SELECT * FROM (VALUES ((SELECT MIN(a) FROM test), 2, 3), ((SELECT MAX(b) FROM test), 2, 3)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3}));

	// value list with different types
	result = con.Query("SELECT * FROM (VALUES ('hello', 2), (1 + 3, '5'), (DATE '1992-09-20', 3)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "4", "1992-09-20"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 5, 3}));

	// value list with NULLs
	result = con.Query("SELECT * FROM (VALUES (DATE '1992-09-20', 3), (NULL, NULL)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 9, 20), Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, Value()}));

	// only NULLs
	result = con.Query("SELECT * FROM (VALUES (NULL, NULL)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// nulls first and then integers
	result = con.Query("SELECT * FROM (VALUES (NULL, NULL), (3, 4)) v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4}));

	// this does not work: type is chosen by first value
	REQUIRE_FAIL(con.Query("SELECT * FROM (VALUES (3), ('hello')) v1;"));
	// this also doesn't work: NULL defaults to integer type
	REQUIRE_FAIL(con.Query("SELECT * FROM (VALUES (NULL), ('hello')) v1;"));
	// unbalanced value list is not allowed
	REQUIRE_FAIL(con.Query("SELECT * FROM (VALUES (1, 2, 3), (1,2)) v1;"));
	// default in value list is not allowed
	REQUIRE_FAIL(con.Query("SELECT * FROM (VALUES (DEFAULT, 2, 3), (1,2)) v1;"));

	// VALUES list for INSERT
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE varchars(v VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO varchars VALUES (1), ('hello'), (DEFAULT);"));

	result = con.Query("SELECT * FROM varchars ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "1", "hello"}));

	// too many columns provided
	REQUIRE_FAIL(con.Query("INSERT INTO varchars VALUES (1, 2), ('hello', 3), (DEFAULT, DEFAULT);"));
	REQUIRE_FAIL(con.Query("INSERT INTO varchars (v) VALUES (1, 2), ('hello', 3), (DEFAULT, DEFAULT);"));
	REQUIRE_FAIL(con.Query("INSERT INTO varchars (v) VALUES (1, 2), ('hello'), (DEFAULT, DEFAULT);"));
	// operation on default not allowed
	REQUIRE_FAIL(con.Query("INSERT INTO varchars (v) VALUES (DEFAULT IS NULL);"));
}
