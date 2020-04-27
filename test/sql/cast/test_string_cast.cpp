#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test string casts", "[cast]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// boolean -> string
	result = con.Query("SELECT (1=1)::VARCHAR, (1=0)::VARCHAR, NULL::BOOLEAN::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"true"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"false"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	// tinyint -> string
	result = con.Query("SELECT 1::TINYINT::VARCHAR, 12::TINYINT::VARCHAR, (-125)::TINYINT::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"12"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-125"}));
	// smallint -> string
	result = con.Query("SELECT 1::SMALLINT::VARCHAR, 12442::SMALLINT::VARCHAR, (-32153)::SMALLINT::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"12442"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-32153"}));
	// integer -> string
	result = con.Query("SELECT 1::INTEGER::VARCHAR, 12442952::INTEGER::VARCHAR, (-2000000111)::INTEGER::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"12442952"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-2000000111"}));
	// bigint -> string
	result =
	    con.Query("SELECT 1::BIGINT::VARCHAR, 1244295295289253::BIGINT::VARCHAR, (-2000000111551166)::BIGINT::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1244295295289253"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-2000000111551166"}));
	// float -> string
	result = con.Query("SELECT 2::FLOAT::VARCHAR, 0.5::FLOAT::VARCHAR, (-128.5)::FLOAT::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"2.0"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"0.5"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-128.5"}));
	// double -> string
	result = con.Query("SELECT 2::DOUBLE::VARCHAR, 0.5::DOUBLE::VARCHAR, (-128.5)::DOUBLE::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"2.0"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"0.5"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-128.5"}));
}
