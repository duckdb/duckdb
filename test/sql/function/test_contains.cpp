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
TEST_CASE("Contains Instr test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT contains_instr(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test second letter
	result = con.Query("SELECT contains_instr(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test last letter
	result = con.Query("SELECT contains_instr(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, Value(nullptr)}));

	// Test multiple letters
	result = con.Query("SELECT contains_instr(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters in the middle
	result = con.Query("SELECT contains_instr(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters at the end
	result = con.Query("SELECT contains_instr(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test no match
	result = con.Query("SELECT contains_instr(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, Value(nullptr)}));

	// Test matching needle in multiple rows
	result = con.Query("SELECT contains_instr(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, Value(nullptr)}));

	// Test NULL constant in different places
	result = con.Query("SELECT contains_instr(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_instr(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_instr(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
}

/* Inspired by the substring test case and C language UTF-8 tests
 *
 */
TEST_CASE("Contains Instr test with UTF8", "[function]") {
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
	result = con.Query("SELECT contains_instr(s,'\xc3\xa1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, false}));

	// Test a sentence with an UTF-8
	result = con.Query("SELECT contains_instr(s,'ol\xc3\xa1 mundo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, false}));

	// Test an entire UTF-8 word
	result = con.Query("SELECT contains_instr(s,'\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, false}));

	// Test a substring of the haystack from the beginning
	result = con.Query("SELECT contains_instr(s,'two \xc3\xb1 thr') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a single UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_instr(s,'\xc3\xb1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a multiple UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_instr(s,'\xE2\x82\xA1 four \xF0\x9F\xA6\x86 e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a substring of the haystack from the middle to the end
	result = con.Query("SELECT contains_instr(s,'\xF0\x9F\xA6\x86 end') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));
}

//------------------ CONTAINS KMP ---------------------------------
TEST_CASE("Contains KMP test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT contains_kmp(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test second letter
	result = con.Query("SELECT contains_kmp(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test last letter
	result = con.Query("SELECT contains_kmp(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, Value(nullptr)}));

	// Test multiple letters
	result = con.Query("SELECT contains_kmp(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters in the middle
	result = con.Query("SELECT contains_kmp(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters at the end
	result = con.Query("SELECT contains_kmp(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test no match
	result = con.Query("SELECT contains_kmp(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, Value(nullptr)}));

	// Test matching needle in multiple rows
	result = con.Query("SELECT contains_kmp(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, Value(nullptr)}));

	// Test NULL constant in different places
	result = con.Query("SELECT contains_kmp(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_kmp(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_kmp(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
}

/* Inspired by the substring test case and C language UTF-8 tests
 *
 */
TEST_CASE("Contains KMP test with UTF8", "[function]") {
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
	result = con.Query("SELECT contains_kmp(s,'\xc3\xa1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, false}));

	// Test a sentence with an UTF-8
	result = con.Query("SELECT contains_kmp(s,'ol\xc3\xa1 mundo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, false}));

	// Test an entire UTF-8 word
	result = con.Query("SELECT contains_kmp(s,'\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, false}));

	// Test a substring of the haystack from the beginning
	result = con.Query("SELECT contains_kmp(s,'two \xc3\xb1 thr') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a single UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_kmp(s,'\xc3\xb1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a multiple UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_kmp(s,'\xE2\x82\xA1 four \xF0\x9F\xA6\x86 e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a substring of the haystack from the middle to the end
	result = con.Query("SELECT contains_kmp(s,'\xF0\x9F\xA6\x86 end') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));
}

//------------------ CONTAINS BM ---------------------------------
TEST_CASE("Contains BM test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT contains_bm(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test second letter
	result = con.Query("SELECT contains_bm(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test last letter
	result = con.Query("SELECT contains_bm(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, Value(nullptr)}));

	// Test multiple letters
	result = con.Query("SELECT contains_bm(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters in the middle
	result = con.Query("SELECT contains_bm(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters at the end
	result = con.Query("SELECT contains_bm(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test no match
	result = con.Query("SELECT contains_bm(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, Value(nullptr)}));

	// Test matching needle in multiple rows
	result = con.Query("SELECT contains_bm(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, Value(nullptr)}));

	// Test NULL constant in different places
	result = con.Query("SELECT contains_bm(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_bm(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_bm(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
}

/* Inspired by the substring test case and C language UTF-8 tests
 *
 */
TEST_CASE("Contains BM test with UTF8", "[function]") {
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
	result = con.Query("SELECT contains_bm(s,'\xc3\xa1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, false}));

	// Test a sentence with an UTF-8
	result = con.Query("SELECT contains_bm(s,'ol\xc3\xa1 mundo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, false}));

	// Test an entire UTF-8 word
	result = con.Query("SELECT contains_bm(s,'\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, false}));

	// Test a substring of the haystack from the beginning
	result = con.Query("SELECT contains_bm(s,'two \xc3\xb1 thr') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a single UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_bm(s,'\xc3\xb1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a multiple UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_bm(s,'\xE2\x82\xA1 four \xF0\x9F\xA6\x86 e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a substring of the haystack from the middle to the end
	result = con.Query("SELECT contains_bm(s,'\xF0\x9F\xA6\x86 end') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));
}

//------------------ CONTAINS STRSTR ---------------------------------
TEST_CASE("Contains STRSTR test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT contains_strstr(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test second letter
	result = con.Query("SELECT contains_strstr(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test last letter
	result = con.Query("SELECT contains_strstr(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, Value(nullptr)}));

	// Test multiple letters
	result = con.Query("SELECT contains_strstr(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters in the middle
	result = con.Query("SELECT contains_strstr(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test multiple letters at the end
	result = con.Query("SELECT contains_strstr(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, Value(nullptr)}));

	// Test no match
	result = con.Query("SELECT contains_strstr(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, Value(nullptr)}));

	// Test matching needle in multiple rows
	result = con.Query("SELECT contains_strstr(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, Value(nullptr)}));

	// Test NULL constant in different places
	result = con.Query("SELECT contains_strstr(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_strstr(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
	result = con.Query("SELECT contains_strstr(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr), Value(nullptr)}));
}

/* Inspired by the substring test case and C language UTF-8 tests
 *
 */
TEST_CASE("Contains STRSTR test with UTF8", "[function]") {
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
	result = con.Query("SELECT contains_strstr(s,'\xc3\xa1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, false, false}));

	// Test a sentence with an UTF-8
	result = con.Query("SELECT contains_strstr(s,'ol\xc3\xa1 mundo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false, false}));

	// Test an entire UTF-8 word
	result = con.Query("SELECT contains_strstr(s,'\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, false}));

	// Test a substring of the haystack from the beginning
	result = con.Query("SELECT contains_strstr(s,'two \xc3\xb1 thr') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a single UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_strstr(s,'\xc3\xb1') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a multiple UTF8 substring of the haystack in the middle
	result = con.Query("SELECT contains_strstr(s,'\xE2\x82\xA1 four \xF0\x9F\xA6\x86 e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));

	// Test a substring of the haystack from the middle to the end
	result = con.Query("SELECT contains_strstr(s,'\xF0\x9F\xA6\x86 end') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, true}));
}
