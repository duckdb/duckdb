#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test DEFAULT in tables", "[default]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// no default specified: write NULL value
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (3);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (DEFAULT, DEFAULT);"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, Value()}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));

	// no default specified: default is NULL value
	// but we set column to NOT NULL
	// now insert should fail
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER NOT NULL, b INTEGER);"));
	REQUIRE_FAIL(con.Query("INSERT INTO test (b) VALUES (3);"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));

	// simple default: constant value
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT 1, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (3);"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));

	// default as expression
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT 1+1, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (3);"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));

	// default with insert from query
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT 1+1, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) SELECT 3"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));

	// default from sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT nextval('seq'), b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (2), (4), (6), (2), (4);"));
	result = con.Query("SELECT * FROM test ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 4, 6, 2, 4}));
	// // cannot drop sequence now
	// REQUIRE_FAIL(con.Query("DROP SEQUENCE seq"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	// after dropping table we can drop seq
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq"));

	// test default with update
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT nextval('seq'), b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=DEFAULT"));
	result = con.Query("SELECT * FROM test ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	// cannot use subquery in DEFAULT expression
	REQUIRE_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT (SELECT 42), b INTEGER);"));
	// aggregate functions are not allowed in DEFAULT expressions
	REQUIRE_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT SUM(42), b INTEGER);"));
	// window functions are not allowed in DEFAULT expressions
	REQUIRE_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT row_number() OVER (), b INTEGER);"));
	// default value must be scalar expression
	REQUIRE_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT b+1, b INTEGER);"));

	// test default with random
	unique_ptr<MaterializedQueryResult> result_tmp;
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a DOUBLE DEFAULT random(), b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (2);"));
	result = con.Query("SELECT a FROM test WHERE b = 1;");
	result_tmp = move(result);
	result = con.Query("SELECT a FROM test WHERE b = 2;");
	REQUIRE(!result->Equals(*result_tmp));
}
