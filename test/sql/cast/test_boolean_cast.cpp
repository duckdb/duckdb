#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test boolean casts", "[cast]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// boolean -> string casts
	result = con.Query("SELECT CAST(1=1 AS VARCHAR)");
	REQUIRE(CHECK_COLUMN(result, 0, {"true"}));
	result = con.Query("SELECT CAST(1=0 AS VARCHAR)");
	REQUIRE(CHECK_COLUMN(result, 0, {"false"}));
	// string -> boolean casts
	result = con.Query("SELECT CAST('true' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT CAST('t' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT CAST('TRUE' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT CAST('false' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT CAST('f' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT CAST('FALSE' AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	REQUIRE_FAIL(con.Query("SELECT CAST('12345' AS BOOLEAN)"));

	// varchar -> integer -> boolean
	result = con.Query("SELECT CAST(CAST('12345' AS INTEGER) AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT CAST(CAST('0' AS INTEGER) AS BOOLEAN)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// varchar -> numeric -> boolean casts
	vector<string> types = {"tinyint", "smallint", "integer", "bigint", "decimal"};
	for (auto &type : types) {
		result = con.Query("SELECT CAST(CAST('1' AS " + type + ") AS BOOLEAN)");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT CAST(CAST('0' AS " + type + ") AS BOOLEAN)");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
}
