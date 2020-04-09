#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test table_info pragma", "[pragma]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER DEFAULT 1+3, j INTEGER)"));

	// PRAGMA table_info(table) returns information on the table
	result = con.Query("PRAGMA table_info('integers');");
	REQUIRE_NO_FAIL(*result);
	// cid
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	// name
	REQUIRE(CHECK_COLUMN(result, 1, {"i", "j"}));
	// types
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));
	// NOT NULL
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	// DEFAULT VALUE
	REQUIRE(CHECK_COLUMN(result, 4, {"1 + 3", Value()}));
	// PRIMARY KEY
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	REQUIRE_FAIL(con.Query("PRAGMA table_info('nonexistant_table');"));

	// table_info on view does not work
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW vintegers AS SELECT 42"));
	REQUIRE_FAIL(con.Query("PRAGMA table_info('vintegers')"));
}
