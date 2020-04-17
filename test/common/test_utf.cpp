#include "catch.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void test_valid_str(Vector &a, const char *str) {
	Value s(str);
	REQUIRE_NOTHROW(a.SetValue(0, s));
	REQUIRE(a.GetValue(0) == s);
}

TEST_CASE("UTF8 error checking", "[utf8]") {
	Vector a(TypeId::VARCHAR);

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

TEST_CASE("UTF8 NFC tests", "[utf8]") {
	// check NFC equivalence in Value API
	REQUIRE(Value("a") == Value("a"));
	REQUIRE(Value("a") != Value("b"));
	REQUIRE(Value("\xc3\xbc") == Value("\xc3\xbc"));
	REQUIRE(Value("\xc3\xbc") == Value("\x75\xcc\x88"));

	// also in SQL
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	unique_ptr<QueryResult> result;

	result = con.Query("SELECT 'a'='a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '\xc3\xbc'='\xc3\xbc'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '\xc3\xbc'='\x75\xcc\x88'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	// also through appenders
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings (s STRING)"));
	Appender appender(con, DEFAULT_SCHEMA, "strings");
	appender.BeginRow();
	appender.Append("\x75\xcc\x88");
	appender.EndRow();
	appender.Close();

	result = con.Query("SELECT s = '\xc3\xbc' FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT s = '\x75\xcc\x88' FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	// related to bug 539
	REQUIRE_NO_FAIL(con.Query("\x49\x4E\x53\x45\x52\x54\x20\x49\x4E\x54\x4F\x20\x73\x74\x72\x69\x6E\x67\x73\x20\x56\x41"
	                          "\x4C\x55\x45\x53\x28\x27\x61\x27\x29"));
	REQUIRE_NO_FAIL(con.Query("\x49\x4E\x53\x45\x52\x54\x20\x49\x4E\x54\x4F\x20\x73\x74\x72\x69\x6E\x67\x73\x20\x56\x41"
	                          "\x4C\x55\x45\x53\x28\x27\x3F\x27\x29"));

	// also through CSV reader
}
