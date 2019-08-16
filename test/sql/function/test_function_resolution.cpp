#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void TestAddition(Connection &con, string type) {
	unique_ptr<QueryResult> result;

	result = con.Query("SELECT 1::" + type + " + 1::TINYINT");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT 1::" + type + " + 1::SMALLINT");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT 1::" + type + " + 1::INT");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT 1::" + type + " + 1::BIGINT");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT 1::" + type + " + 1::REAL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::FLOAT(2.0f)}));
	result = con.Query("SELECT 1::" + type + " + 1::DOUBLE");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DOUBLE(2.0)}));
	REQUIRE_FAIL(con.Query("SELECT 1::" + type + " + 1::VARCHAR"));
}

TEST_CASE("Test type resolution of functions", "[function]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// type resolution of addition
	TestAddition(con, "TINYINT");
	TestAddition(con, "SMALLINT");
	TestAddition(con, "INTEGER");
	TestAddition(con, "BIGINT");
	TestAddition(con, "REAL");
	TestAddition(con, "DOUBLE");
}

TEST_CASE("Test type resolution of function with parameter expressions", "[function]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// can deduce type of prepared parameter here
	auto prepared = con.Prepare("select 1 + $1");
	REQUIRE(prepared->error.empty());

	result = prepared->Execute(1);
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}
