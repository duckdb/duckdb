#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test dependencies with multiple connections", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// single schema and dependencies
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	REQUIRE_FAIL(con.Query("DROP SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA s1 CASCADE"));
	REQUIRE_FAIL(con.Query("SELECT * FROM s1.integers"));

	// schemas and dependencies
	// create a schema and a table inside the schema
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.integers(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// drop the table in con1
	REQUIRE_NO_FAIL(con.Query("DROP TABLE s1.integers"));
	// we can't drop the schema from con2 because the table still exists for con2!
	REQUIRE_FAIL(con2.Query("DROP SCHEMA s1"));
	// now rollback the table drop
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	// the table exists again
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	// try again
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// drop the schema entirely now
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA s1 CASCADE"));
	// we can still query the table from con2
	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM s1.integers"));
	// even after we commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM s1.integers"));
	// however if we end the transaction in con2 the schema is gone
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	REQUIRE_FAIL(con2.Query("CREATE TABLE s1.dummy(i INTEGER)"));

	// prepared statements and dependencies
	// dependency on a bound table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT * FROM integers"));
	REQUIRE_NO_FAIL(con2.Query("EXECUTE v"));
	// cannot drop table now
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// but CASCADE drop should work
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers CASCADE"));
	// after the cascade drop the prepared statement is invalidated
	REQUIRE_FAIL(con2.Query("EXECUTE v"));

	// dependency on a sequence
	// REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	// REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT nextval('seq')"));
	// result = con2.Query("EXECUTE v");
	// REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// // cannot drop sequence now
	// REQUIRE_FAIL(con.Query("DROP SEQUENCE seq"));
	// // check that the prepared statement still works
	// result = con2.Query("EXECUTE v");
	// REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// // cascade drop
	// REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq CASCADE"));
	// // after the cascade drop the prepared statement is invalidated
	// REQUIRE_FAIL(con2.Query("EXECUTE v"));
}

// TEST_CASE("Test parallel dependencies in multiple connections", "[catalog]") {
// 	unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
// 	Connection con2(db);


// }
