
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test subqueries", "[subqueries]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// scalar NULL
	REQUIRE(duckdb_query(connection, "SELECT (SELECT 42)", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {42}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT (SELECT (SELECT 42))", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {42}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test (a INTEGER, b INTEGER);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (12, 21)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)",
	                     NULL) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection, "SELECT (SELECT a * 42 FROM test)",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11 * 42}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a*(SELECT 42) FROM test",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11 * 42, 12 * 42, 13 * 42}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d "
	                     "INTEGER, e INTEGER)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(
	    duckdb_query(connection,
	                 "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)",
	                 NULL) == DuckDBSuccess);
	REQUIRE(
	    duckdb_query(connection,
	                 "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)",
	                 NULL) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection, "SELECT c-(SELECT sum(c) FROM t1) FROM t1",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {102 - 208, 106 - 208}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "SELECT CASE WHEN c>(SELECT sum(c)/count(*) FROM t1) "
	                     "THEN a*2 ELSE b*10 END FROM t1",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {1000, 214}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}

//
// EXEC("CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e "
//     "INTEGER)");
// EXEC("INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)");
// EXEC("INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)");
//
////	EXEC("SELECT c-(SELECT sum(c) FROM t1) FROM t1");
////	EXEC("SELECT (SELECT 42)");
//
// EXEC("SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END "
//     "FROM t1 ORDER BY 1");
