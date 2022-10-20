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
	REQUIRE(left.value.upper == right.value.upper);
}

void TestFetchAsDecimal(CAPITester &tester, string query, string type_cast) {
	auto result = tester.Query(StringUtil::Format(query, type_cast));
	REQUIRE_NO_FAIL(*result);

	// (ANYTHING BUT DECIMAL) -> DECIMAL results in 0
	duckdb_decimal expected_res;
	expected_res.scale = 0;
	expected_res.width = 0;
	expected_res.value.lower = 0;
	expected_res.value.upper = 0;

	auto converted_res = result->Fetch<duckdb_decimal>(0, 0);
	CompareDuckDBDecimal(expected_res, converted_res);
}

TEST_CASE("Test CAPI duckdb_decimal_as_properties", "[capi]") {
	CAPITester tester;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	//! From DOUBLE
	TestFetchAsDecimal(tester, "SELECT CAST(123.45678 AS %s)", "DOUBLE");
	//! From FLOAT
	TestFetchAsDecimal(tester, "SELECT CAST(123.45678 AS %s)", "FLOAT");
	//! From HUGEINT
	TestFetchAsDecimal(tester, "SELECT CAST(123124 AS %s)", "HUGEINT");
	//! From BIGINT
	TestFetchAsDecimal(tester, "SELECT CAST(123124 AS %s)", "BIGINT");
	//! From UBIGINT
	TestFetchAsDecimal(tester, "SELECT CAST(123124 AS %s)", "UBIGINT");
	//! From INTEGER
	TestFetchAsDecimal(tester, "SELECT CAST(123124 AS %s)", "INTEGER");
	//! From UINTEGER
	TestFetchAsDecimal(tester, "SELECT CAST(123124 AS %s)", "UINTEGER");
	//! From SMALLINT
	TestFetchAsDecimal(tester, "SELECT CAST(12312 AS %s)", "SMALLINT");
	//! From USMALLINT
	TestFetchAsDecimal(tester, "SELECT CAST(12312 AS %s)", "USMALLINT");
	//! From TINYINT
	TestFetchAsDecimal(tester, "SELECT CAST(-123 AS %s)", "TINYINT");
	//! From UTINYINT
	TestFetchAsDecimal(tester, "SELECT CAST(255 AS %s)", "UTINYINT");
	//! From VARCHAR
	TestFetchAsDecimal(tester, "SELECT CAST(123124.2342 AS %s)", "VARCHAR");
}
