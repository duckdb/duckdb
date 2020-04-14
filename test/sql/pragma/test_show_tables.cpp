#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test SHOW/DESCRIBE tables", "[pragma]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT DATE '1992-01-01' AS k"));

	// SHOW and DESCRIBE are aliases
	result = con.Query("SHOW TABLES");
	REQUIRE(CHECK_COLUMN(result, 0, {"integers", "v1"}));
	result = con.Query("DESCRIBE TABLES");
	REQUIRE(CHECK_COLUMN(result, 0, {"integers", "v1"}));
	// internally they are equivalent to PRAGMA SHOW_TABLES();
	result = con.Query("PRAGMA show_tables");
	REQUIRE(CHECK_COLUMN(result, 0, {"integers", "v1"}));

	result = con.Query("SHOW integers");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES", "YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), Value()}));
	// equivalent to PRAGMA SHOW('integers')
	result = con.Query("PRAGMA SHOW('integers')");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES", "YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), Value()}));

	// we can also describe views
	result = con.Query("DESCRIBE v1");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"k"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"DATE"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));
}
