#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test index creation statements with multiple connections", "[join]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	DuckDBConnection con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));
	for (size_t i = 0; i < 3000; i++) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO integers VALUES (" + to_string(i + 10) + ", " + to_string(i + 12) + ")"));
	}

	// both con and con2 start a transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// con2 updates the integers array before index creation
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=1"));

	// con creates an index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	// con should see the old state
	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// con2 should see the updated state
	result = con2.Query("SELECT j FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now we commit con
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// con should still see the old state
	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// after commit of con2 - con should see the old state
	result = con.Query("SELECT j FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now we update the index again, this time after index creation
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=7 WHERE i=4"));
	// the new state should be visible
	result = con.Query("SELECT j FROM integers WHERE i=7");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
}

TEST_CASE("Index creation on an expression", "[join]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	for (size_t i = 0; i < 3000; i++) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO integers VALUES (" + to_string((int)(i - 1500)) + ", " + to_string(i + 12) + ")"));
	}

	result = con.Query("SELECT j FROM integers WHERE abs(i)=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1511, 1513}));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(abs(i))"));

	result = con.Query("SELECT j FROM integers WHERE abs(i)=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1511, 1513}));
}

TEST_CASE("Drop Index", "[drop]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (7)"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (5)"));
	result = con.Query("SELECT * FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (8)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	result = con.Query("SELECT * FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("DROP INDEX i_index;");
	result = con.Query("SELECT * FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	result = con.Query("SELECT * FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}

TEST_CASE("Open Range Queries", "[openrange]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	for (size_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(i) + ")"));
	}
	// REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	result = con.Query("SELECT sum(i) FROM integers WHERE i>9");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT sum(i) FROM integers WHERE 9<i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i>=10");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i>7");
	REQUIRE(CHECK_COLUMN(result, 0, {17}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i>=7");
	REQUIRE(CHECK_COLUMN(result, 0, {24}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i<3");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i<=3");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i<0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i=0");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i > 7 and  i>3");
	REQUIRE(CHECK_COLUMN(result, 0, {17}));
	result = con.Query("SELECT sum(i) FROM integers WHERE  i >= 7 and i > 7");
	REQUIRE(CHECK_COLUMN(result, 0, {17}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i<=3 and i < 3");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
}

TEST_CASE("Closed Range Queries", "[closerange]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	for (size_t i = 0; i < 10; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(i) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	result = con.Query("SELECT sum(i) FROM integers WHERE i> 5 and i>9 ");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}
