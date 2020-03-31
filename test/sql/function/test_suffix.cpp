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
TEST_CASE("Suffix test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	SECTION("Short string (4bytes)") {
		result = con.Query("SELECT suffix('abcd', 'd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcd', 'cd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcd', 'bcd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcd', 'abcd')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcd', 'X')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}

	SECTION("Medium string (8bytes)") {
		result = con.Query("SELECT suffix('abcdefgh', 'h')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefgh', 'gh')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefgh', 'fgh')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefgh', 'efgh')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefgh', 'defgh')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefgh', 'X')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT suffix('abcdefgh', 'abcdefgh')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

	SECTION("Long string (> 15bytes)") {
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'z')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'yz')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'xyz')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'wxyz')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'vwxyz')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'X')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		result = con.Query("SELECT suffix('abcdefghijklmnopqrstuvwxyz', 'defghijklmnopqrstuvwxyz')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

	SECTION("Empty string and suffix") {
		result = con.Query("SELECT suffix('', 'aaa')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		result = con.Query("SELECT suffix('aaa', '')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

	SECTION("NULL string and suffix") {
		result = con.Query("SELECT suffix(NULL, 'aaa')");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr)}));

		result = con.Query("SELECT suffix('aaa', NULL)");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr)}));

		result = con.Query("SELECT suffix(NULL, NULL)");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr)}));
	}

	SECTION("Suffix test with UTF8") {
		// inverse "átomo" (atom)
		result = con.Query("SELECT suffix('omot\xc3\xa1', '\xc3\xa1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('omot\xc3\xa1', 'á')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('omot\xc3\xa1', 'a')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		// inverse "olá mundo" (hello world)
		result = con.Query("SELECT suffix('mundo ol\xc3\xa1', 'ol\xc3\xa1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('mundo ol\xc3\xa1', 'olá')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('mundo olá', 'mundo olá')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('mundo ol\xc3\xa1', 'ola')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		// eftñ
		result = con.Query("SELECT suffix('\x65\x66\x74\xc3\xb1', '\xc3\xb1')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		//ñeft
		result = con.Query("SELECT suffix('\xc3\xb1\x65\x66\x74', 'ñeft')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix('\xc3\xb1\x65\x66\x74', 'neft')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));

		// two ñ three ₡ four 🦆 end
		string str_utf8 = "'two \xc3\xb1 three \xE2\x82\xA1 four \xF0\x9F\xA6\x86 end'";

		result = con.Query("SELECT suffix(" + str_utf8 + ", '\xF0\x9F\xA6\x86 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix(" + str_utf8 + ", '🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));

		result = con.Query("SELECT suffix(" + str_utf8 + ", 'three \xE2\x82\xA1 four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix(" + str_utf8 + ", 'three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));

		result = con.Query("SELECT suffix(" + str_utf8 + ", 'two \xc3\xb1 three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix(" + str_utf8 + ", 'two ñ three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT suffix(" + str_utf8 + ", 'two ñ three \xE2\x82\xA1 four \xF0\x9F\xA6\x86 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));

		result = con.Query("SELECT suffix(" + str_utf8 + ", 'two n three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT suffix(" + str_utf8 + ", 'XXXtwo ñ three ₡ four 🦆 end')");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
}
