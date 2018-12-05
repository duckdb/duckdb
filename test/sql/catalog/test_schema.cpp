#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Schema creation/deletion", "[catalog]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create and drop an empty schema
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA test;"));

	// create the schema again
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
	// duplicate schema
	REQUIRE_FAIL(con.Query("CREATE SCHEMA test;"));
	// if not exists ignores error
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA IF NOT EXISTS test;"));

	// create table inside schema that exists should succeed
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.hello(i INTEGER);"));
	// create table inside schema that does not exist should fail
	REQUIRE_FAIL(con.Query("CREATE TABLE test2.hello(i INTEGER);"));

	// use the table in queries
	// insert into table
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test.hello VALUES (2), (3), (4)"));
	// select from table without schema specified should fail
	REQUIRE_FAIL(con.Query("SELECT * FROM hello"));

	// with schema specified should succeed
	result = con.Query("SELECT * FROM test.hello");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// drop schema with dependencies should fail
	REQUIRE_FAIL(con.Query("DROP SCHEMA test;"));
	// unless we use cascade to drop
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA test CASCADE;"));
	// drop schema if exists should not fail if schema does not exist
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA IF EXISTS test;"));
}

TEST_CASE("Schema creation/deletion with transactions", "[catalog]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create a schema with a table
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.hello(i INTEGER);"));

	// in one transaction drop the table and then the schema (without cascade)
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test.hello;"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA test;"));
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// now work with multiple connections
	DuckDBConnection con2(db);

	// create the same schema
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.hello(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test.hello VALUES (2), (3), (4)"));

	// begin the transactions
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// con1 drops the schema and commits it
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test.hello;"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA test;"));
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// con2 queries the schema (should still work)
	result = con2.Query("SELECT * FROM test.hello");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// now con2 finishes the transaction and tries again
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK;"));
	REQUIRE_FAIL(con2.Query("SELECT * FROM test.hello"));
}
