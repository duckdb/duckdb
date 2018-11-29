
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test index creation statements", "[join]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	DuckDBConnection con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));
	for (size_t i = 0; i < 3000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" +
		                          to_string(i + 10) + ", " + to_string(i + 12) +
		                          ")"));
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
