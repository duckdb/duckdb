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

	// table_info on view also works
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT 42::INTEGER AS a, 'hello' AS b"));
	result = con.Query("PRAGMA table_info('v1')");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "VARCHAR"}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	// view with explicit aliases
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v2(c) AS SELECT 42::INTEGER AS a, 'hello' AS b"));
	result = con.Query("PRAGMA table_info('v2')");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"c", "b"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "VARCHAR"}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v3(c, d) AS SELECT DATE '1992-01-01', 'hello' AS b"));
	result = con.Query("PRAGMA table_info('v3')");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"c", "d"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"DATE", "VARCHAR"}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	// test table info with other schemas
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW test.v1 AS SELECT 42::INTEGER AS a, 'hello' AS b"));
	result = con.Query("PRAGMA table_info('test.v1')");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "VARCHAR"}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	REQUIRE_FAIL(con.Query("PRAGMA table_info('nonexistant_table');"));
}
