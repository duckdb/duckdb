#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test table_info pragma", "[pragma]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));

	// PRAGMA table_info(table) returns information on the table
	REQUIRE_NO_FAIL(result = con.Query("PRAGMA table_info('integers');"));
	// cid
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	// name
	REQUIRE(CHECK_COLUMN(result, 1, {"i", "j"}));
	// types
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));
	// NOT NULL
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	// DEFAULT VALUE
	REQUIRE(CHECK_COLUMN(result, 4, {false, false}));
	// PRIMARY KEY
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	REQUIRE_FAIL(con.Query("PRAGMA table_info('nonexistant_table');"));
}
