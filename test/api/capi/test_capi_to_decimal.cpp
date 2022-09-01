#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

int64_t Difference(int64_t left, int64_t right) {
	return abs(left - right);
}

void CompareDuckDBDecimal(const duckdb_decimal &left, const duckdb_decimal &right) {
	REQUIRE(left.scale == right.scale);
	REQUIRE(left.width == right.width);

	auto difference = Difference(left.value.lower, right.value.lower);
	if (difference == 0) {
		auto left_str = Hugeint::ToString(*(hugeint_t *)(&left.value));
		auto right_str = Hugeint::ToString(*(hugeint_t *)(&right.value));
		REQUIRE(!strcmp(left_str.c_str(), right_str.c_str()));
	} else {
		REQUIRE(difference < 2);
	}
	REQUIRE(left.value.upper == right.value.upper);
}

void TestTypeConversion(CAPITester &tester, string query, string type_cast) {
	auto result = tester.Query(StringUtil::Format(query, type_cast));
	REQUIRE_NO_FAIL(*result);
	auto decimal_result = tester.Query(StringUtil::Format(query, "DECIMAL"));
	REQUIRE_NO_FAIL(*decimal_result);
	auto expected_res = decimal_result->Fetch<duckdb_decimal>(0, 0);
	auto converted_res = result->FetchAsDecimal(0, 0, expected_res.width, expected_res.scale);
	CompareDuckDBDecimal(expected_res, converted_res);
}

TEST_CASE("Test CAPI duckdb_decimal_as_properties", "[capi]") {
	CAPITester tester;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	////! From DOUBLE
	// TestTypeConversion(tester, "SELECT CAST(123.45678 AS %s)", "DOUBLE");
	////! From FLOAT
	// TestTypeConversion(tester, "SELECT CAST(123.45678 AS %s)", "FLOAT");
	////! From HUGEINT
	// TestTypeConversion(tester, "SELECT CAST(123124 AS %s)", "HUGEINT");
	////! From BIGINT
	// TestTypeConversion(tester, "SELECT CAST(123124 AS %s)", "BIGINT");
	////! From UBIGINT
	// TestTypeConversion(tester, "SELECT CAST(123124 AS %s)", "UBIGINT");
	////! From INTEGER
	// TestTypeConversion(tester, "SELECT CAST(123124 AS %s)", "INTEGER");
	////! From UINTEGER
	// TestTypeConversion(tester, "SELECT CAST(123124 AS %s)", "UINTEGER");
	////! From SMALLINT
	// TestTypeConversion(tester, "SELECT CAST(12312 AS %s)", "SMALLINT");
	////! From USMALLINT
	// TestTypeConversion(tester, "SELECT CAST(12312 AS %s)", "USMALLINT");
	//! From TINYINT
	TestTypeConversion(tester, "SELECT CAST(-123 AS %s)", "TINYINT");
	////! From UTINYINT
	// TestTypeConversion(tester, "SELECT CAST(255 AS %s)", "UTINYINT");
	////! From VARCHAR
	// TestTypeConversion(tester, "SELECT CAST(123124.2342 AS %s)", "VARCHAR");
}
