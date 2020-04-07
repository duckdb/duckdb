#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

/* Test Case disclaimer
 *
 *  Assertions built using the Domain Testing technique
 *  at: https://bbst.courses/wp-content/uploads/2018/01/Kaner-Intro-to-Domain-Testing-2018.pdf
 *
 */
TEST_CASE("Contains test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT contains(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test second letter
	result = con.Query("SELECT contains(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test last letter
	result = con.Query("SELECT contains(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, Value(nullptr)}));

	// Test multiple letters
	result = con.Query("SELECT contains(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters in the middle
	result = con.Query("SELECT contains(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters at the end
	result = con.Query("SELECT contains(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test no match
	result = con.Query("SELECT contains(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, Value(nullptr)}));

	// Test matching needle in multiple rows
	result = con.Query("SELECT contains(s,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, Value(nullptr)}));

	// Test NULL constant in different places
	result = con.Query("SELECT contains(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));

	// Test empty pattern
	result = con.Query("SELECT contains(s,'') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, Value(nullptr)}));
}

/* Inspired by the substring test case and C language UTF-8 tests
 *
 */
TEST_CASE("Contains test with UTF8", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	string atomo = "\xc3\xa1tomo";                                     // length 6
	string portg = "ol\xc3\xa1 mundo";                                 // olá mundo length 9
	string nihao = "\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c"; //你好世界 length 4
	string potpourri = "two \xc3\xb1 three \xE2\x82\xA1 four \xF0\x9F\xA6\x86 end";

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('" + atomo + "')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('" + portg + "')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('" + nihao + "')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('" + potpourri + "')"));

	// Test one matching UTF8 letter
	result = con.Query("SELECT contains(s,'\xc3\xa1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, false}));

	// Test a sentence with an UTF-8
	result = con.Query("SELECT contains(s,'ol\xc3\xa1 mundo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, false}));

	// Test an entire UTF-8 word
	result = con.Query("SELECT contains(s,'\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, false}));

	// Test a substring of the haystack from the beginning
	result = con.Query("SELECT contains(s,'two \xc3\xb1 thr') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a single UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains(s,'\xc3\xb1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a multiple UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains(s,'\xE2\x82\xA1 four \xF0\x9F\xA6\x86 e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a substring of the haystack from the middle to the end
	result = con.Query("SELECT contains(s,'\xF0\x9F\xA6\x86 end') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));
}
