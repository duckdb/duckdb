#include "catch.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static void test_valid_str(Vector &a, const char *str) {
	Value s(str);
	REQUIRE_NOTHROW(a.SetValue(0, s));
	REQUIRE(a.GetValue(0) == s);
}

TEST_CASE("UTF8 error checking", "[utf8]") {
	Vector a(LogicalType::VARCHAR);

	test_valid_str(a, "a");
	test_valid_str(a, "\xc3\xb1");
	test_valid_str(a, "\xE2\x82\xA1");
	test_valid_str(a, "\xF0\x9F\xA6\x86"); // a duck!
	test_valid_str(a, "\xf0\x90\x8c\xbc");

	REQUIRE_THROWS(a.SetValue(0, Value("\xc3\x28")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xa0\xa1")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xe2\x28\xa1")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xe2\x82\x28")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xf0\x28\x8c\xbc")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xf0\x90\x28\xbc")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xf0\x28\x8c\x28")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xf8\xa1\xa1\xa1\xa1")));
	REQUIRE_THROWS(a.SetValue(0, Value("\xfc\xa1\xa1\xa1\xa1\xa1")));
}

TEST_CASE("UTF8 verification errors escape invalid bytes", "[utf8]") {
	const string invalid = "prefix\xff\xfe";
	try {
		string_t(invalid).ForceVerify();
		FAIL("Invalid UTF8 was accepted");
	} catch (const InternalException &ex) {
		ErrorData error(ex);
		REQUIRE(error.Type() == ExceptionType::INTERNAL);
		REQUIRE(error.RawMessage() == "Invalid UTF8 found in string - prefix\\xFF\\xFE");
		REQUIRE_NOTHROW(string_t(error.Message()).ForceVerify());
	}
}
