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
TEST_CASE("Prefix test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	SECTION("Early out prefix") {
		result = con.Query("SELECT prefix('abcd', 'a')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcd', 'ab')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcd', 'abc')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcd', 'abcd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcd', 'b')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}

	SECTION("Inlined string") {
		result = con.Query("SELECT prefix('abcdefgh', 'a')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefgh', 'ab')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefgh', 'abc')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefgh', 'abcd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefgh', 'abcde')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefgh', 'b')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}

	SECTION("Stored pointer string") {
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'a')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'ab')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'abc')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'abcd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'abcde')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'b')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		result = con.Query("SELECT prefix('abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwx')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

	SECTION("Empty string and prefix") {
		result = con.Query("SELECT prefix('', 'aaa')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		result = con.Query("SELECT prefix('aaa', '')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

	SECTION("Issue #572 alloc exception on empty table") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t0 WHERE PREFIX(t0.c0, '')"));
	}

	SECTION("Prefix test with UTF8") {
		// átomo (atom)
		result = con.Query("SELECT prefix('\xc3\xa1tomo', '\xc3\xa1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('\xc3\xa1tomo', 'á')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('\xc3\xa1tomo', 'a')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		// olá mundo (hello world)
		result = con.Query("SELECT prefix('ol\xc3\xa1 mundo', 'ol\xc3\xa1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('ol\xc3\xa1 mundo', 'olá')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('ol\xc3\xa1 mundo', 'ola')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		//ñeft
		result = con.Query("SELECT prefix('\xc3\xb1\x65\x66\x74', '\xc3\xb1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('\xc3\xb1\x65\x66\x74', 'ñ')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('\xc3\xb1\x65\x66\x74', 'ñeft')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix('\xc3\xb1\x65\x66\x74', 'neft')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		// two ñ three ₡ four 🦆 end
		string str_utf8 = "'two \xc3\xb1 three \xE2\x82\xA1 four \xF0\x9F\xA6\x86 end'";

		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two \xc3\xb1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two n')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ three')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ three \xE2\x82\xA1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ three \xE2\x82\xA1 four \xF0\x9F\xA6\x86')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ three \xE2\x82\xA1 four \xF0\x9F\xA6\x86 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT prefix(" + str_utf8 + ", 'two ñ three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}
}
