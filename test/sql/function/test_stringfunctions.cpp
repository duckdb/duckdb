#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("CONCAT test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks')"));

	result = con.Query("select CONCAT(a, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloSUFFIX", "HuLlDSUFFIX", "MotörHeadSUFFIX"}));

	result = con.Query("select CONCAT('PREFIX', b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"PREFIXWorld", "PREFIX", "PREFIXRÄcks"}));

	result = con.Query("select CONCAT(a, b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorld", "HuLlD", "MotörHeadRÄcks"}));

	result = con.Query("select CONCAT(a, b, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorldSUFFIX", "HuLlDSUFFIX", "MotörHeadRÄcksSUFFIX"}));

	result = con.Query("select CONCAT(a, b, a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorldHello", "HuLlDHuLlD", "MotörHeadRÄcksMotörHead"}));

	result = con.Query("select CONCAT('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')");
	REQUIRE(CHECK_COLUMN(result, 0, {"1234567890"}));

	// concat a long string
	result = con.Query("select '1234567890' || '1234567890', '1234567890' || NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"12345678901234567890"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	result = con.Query("select CONCAT('1234567890', '1234567890'), CONCAT('1234567890', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"12345678901234567890"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1234567890"}));
}

TEST_CASE("CONCAT_WS test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks')"));

	result = con.Query("select CONCAT_WS(',',a, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hello,SUFFIX", "HuLlD,SUFFIX", "MotörHead,SUFFIX"}));

	result = con.Query("select CONCAT_WS('@','PREFIX', b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"PREFIX@World", "PREFIX", "PREFIX@RÄcks"}));

	result = con.Query("select CONCAT_WS('$',a, b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hello$World", "HuLlD", "MotörHead$RÄcks"}));

	result = con.Query("select CONCAT_WS(a, b, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldHelloSUFFIX", "SUFFIX", "RÄcksMotörHeadSUFFIX"}));

	result = con.Query("select CONCAT_WS(a, b, b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldHelloWorld", "", "RÄcksMotörHeadRÄcks"}));

	result = con.Query("select CONCAT_WS('@','1', '2', '3', '4', '5', '6', '7', '8', '9')");
	REQUIRE(CHECK_COLUMN(result, 0, {"1@2@3@4@5@6@7@8@9"}));

	result = con.Query("select CONCAT_WS(b, '[', ']') FROM strings ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"[World]", Value(), "[RÄcks]"}));

	// filters
	result = con.Query("select CONCAT_WS(',', a, 'SUFFIX') FROM strings WHERE a != 'Hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"HuLlD,SUFFIX", "MotörHead,SUFFIX"}));

	// concat WS needs at least two parameters
	REQUIRE_FAIL(con.Query("select CONCAT_WS()"));
	REQUIRE_FAIL(con.Query("select CONCAT_WS(',')"));

	// one entry: just returns the entry
	result = con.Query("select CONCAT_WS(',', 'hello')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));

	// NULL in separator results in null
	result = con.Query("select CONCAT_WS(NULL, 'hello')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	// NULL in data results in empty string
	result = con.Query("select CONCAT_WS(',', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));

	// NULL separator returns in entire column being NULL
	result = con.Query("select CONCAT_WS(NULL, b, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	// NULL in separator is just ignored
	result = con.Query("select CONCAT_WS(',', NULL, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"SUFFIX", "SUFFIX", "SUFFIX"}));

	// empty strings still get split up by the separator
	result = con.Query("select CONCAT_WS(',', '', '')");
	REQUIRE(CHECK_COLUMN(result, 0, {","}));
	result = con.Query("select CONCAT_WS(',', '', '', '')");
	REQUIRE(CHECK_COLUMN(result, 0, {",,"}));

	// but NULLs do not
	result = con.Query("select CONCAT_WS(',', NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	result = con.Query("select CONCAT_WS(',', NULL, NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	result = con.Query("select CONCAT_WS(',', NULL, NULL, 'hello')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));

	// now test for non-constant separators
	result = con.Query("select CONCAT_WS(a, '', NULL, '') FROM strings ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hello", "HuLlD", "MotörHead"}));
	result = con.Query("select CONCAT_WS(a, NULL, '', '') FROM strings ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hello", "HuLlD", "MotörHead"}));

	// now non-constant separator with a mix of constant and non-constant strings to concatenate
	result = con.Query("select CONCAT_WS(a, NULL, b, '') FROM strings ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldHello", "", "RÄcksMotörHead"}));
}

TEST_CASE("UPPER/LOWER test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// unicode
	result = con.Query("select UPPER('áaaá'), UPPER('ö'), LOWER('S̈'), UPPER('ω')");
	REQUIRE(CHECK_COLUMN(result, 0, {"ÁAAÁ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"ö"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"s̈"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Ω"}));

	// greek
	result = con.Query("SELECT UPPER('Αα Ββ Γγ Δδ Εε Ζζ  Ηη Θθ Ιι Κκ Λλ Μμ Νν Ξξ Οο Ππ Ρρ Σσς Ττ Υυ Φφ Χχ Ψψ Ωω'), "
	                   "LOWER('Αα Ββ Γγ Δδ Εε Ζζ  Ηη Θθ Ιι Κκ Λλ Μμ Νν Ξξ Οο Ππ Ρρ Σσς Ττ Υυ Φφ Χχ Ψψ Ωω')");
	REQUIRE(CHECK_COLUMN(result, 0, {"ΑΑ ΒΒ ΓΓ ΔΔ ΕΕ ΖΖ  ΗΗ ΘΘ ΙΙ ΚΚ ΛΛ ΜΜ ΝΝ ΞΞ ΟΟ ΠΠ ΡΡ ΣΣΣ ΤΤ ΥΥ ΦΦ ΧΧ ΨΨ ΩΩ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"αα ββ γγ δδ εε ζζ  ηη θθ ιι κκ λλ μμ νν ξξ οο ππ ρρ σσς ττ υυ φφ χχ ψψ ωω"}));

	// test upper/lower on scalar values
	result = con.Query("select UPPER(''), UPPER('hello'), UPPER('MotörHead'), UPPER(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HELLO"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"MOTÖRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	result = con.Query("select LOWER(''), LOWER('hello'), LOWER('MotörHead'), LOWER(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test ucase/lcase on scalar values
	result = con.Query("select UCASE(''), UCASE('hello'), UCASE('MotörHead'), UCASE(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HELLO"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"MOTÖRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	result = con.Query("select LCASE(''), LCASE('hello'), LCASE('MotörHead'), LCASE(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test on entire tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks')"));

	result = con.Query("select UPPER(a), UCASE(a)  FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "HULLD", "MOTÖRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HELLO", "HULLD", "MOTÖRHEAD"}));

	result = con.Query("select LOWER(a), LCASE(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hulld", "motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hulld", "motörhead"}));

	result = con.Query("select LOWER(b), LCASE(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"world", Value(), "räcks"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"world", Value(), "räcks"}));

	// test with selection vector
	result = con.Query("select UPPER(a), LOWER(a), UCASE(a), LCASE(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "MOTÖRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"HELLO", "MOTÖRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello", "motörhead"}));
}

TEST_CASE("LPAD/RPAD test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test lpad on NULLs
	result = con.Query("select LPAD(NULL, 7, '-'), LPAD('Base', NULL, '-'), LPAD('Base', 7, NULL), "
	                   "LPAD(NULL, NULL, '-'), LPAD(NULL, 7, NULL), LPAD('Base', NULL, NULL), "
	                   "LPAD(NULL, NULL, NULL)");
	for (idx_t col_idx = 0; col_idx < 7; ++col_idx) {
		REQUIRE(CHECK_COLUMN(result, col_idx, {Value()}));
	}

	// test rpad on NULLs
	result = con.Query("select RPAD(NULL, 7, '-'), RPAD('Base', NULL, '-'), RPAD('Base', 7, NULL), "
	                   "RPAD(NULL, NULL, '-'), RPAD(NULL, 7, NULL), RPAD('Base', NULL, NULL), "
	                   "RPAD(NULL, NULL, NULL)");
	for (idx_t col_idx = 0; col_idx < 7; ++col_idx) {
		REQUIRE(CHECK_COLUMN(result, col_idx, {Value()}));
	}

	// test lpad/rpad on scalar values
	result = con.Query("select LPAD('Base', 7, '-'), LPAD('Base', 4, '-'), LPAD('Base', 2, ''), LPAD('Base', -1, '-')");
	REQUIRE(CHECK_COLUMN(result, 0, {"---Base"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Base"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Ba"}));
	REQUIRE(CHECK_COLUMN(result, 3, {""}));

	result = con.Query("select RPAD('Base', 7, '-'), RPAD('Base', 4, '-'), RPAD('Base', 2, ''), RPAD('Base', -1, '-')");
	REQUIRE(CHECK_COLUMN(result, 0, {"Base---"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Base"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Ba"}));
	REQUIRE(CHECK_COLUMN(result, 3, {""}));

	result =
	    con.Query("select LPAD('Base', 7, '-|'), LPAD('Base', 6, '-|'), LPAD('Base', 5, '-|'), LPAD('Base', 4, '-|')");
	REQUIRE(CHECK_COLUMN(result, 0, {"-|-Base"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"-|Base"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"-Base"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Base"}));

	result =
	    con.Query("select RPAD('Base', 7, '-|'), RPAD('Base', 6, '-|'), RPAD('Base', 5, '-|'), RPAD('Base', 4, '-|')");
	REQUIRE(CHECK_COLUMN(result, 0, {"Base-|-"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Base-|"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Base-"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Base"}));

	result = con.Query(
	    "select LPAD('MotörHead', 16, 'RÄcks'), LPAD('MotörHead', 12, 'RÄcks'), LPAD('MotörHead', 10, 'RÄcks')");
	REQUIRE(CHECK_COLUMN(result, 0, {"RÄcksRÄMotörHead"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"RÄcMotörHead"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"RMotörHead"}));

	result = con.Query(
	    "select RPAD('MotörHead', 16, 'RÄcks'), RPAD('MotörHead', 12, 'RÄcks'), RPAD('MotörHead', 10, 'RÄcks')");
	REQUIRE(CHECK_COLUMN(result, 0, {"MotörHeadRÄcksRÄ"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"MotörHeadRÄc"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"MotörHeadR"}));

	// test on entire tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks')"));

	result = con.Query("select LPAD(a, 16, b), RPAD(a, 16, b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldWorldWHello", Value(), "RÄcksRÄMotörHead"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HelloWorldWorldW", Value(), "MotörHeadRÄcksRÄ"}));

	// test with selection vector
	result = con.Query("select LPAD(a, 12, b), RPAD(a, 12, b), UCASE(a), LCASE(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldWoHello", "RÄcMotörHead"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HelloWorldWo", "MotörHeadRÄc"}));

	// test incorrect usage
	REQUIRE_FAIL(con.Query("select LPAD()"));
	REQUIRE_FAIL(con.Query("select LPAD(1)"));
	REQUIRE_FAIL(con.Query("select LPAD(1, 2)"));
	REQUIRE_FAIL(con.Query("select LPAD('Hello', 10, '')"));

	REQUIRE_FAIL(con.Query("select RPAD()"));
	REQUIRE_FAIL(con.Query("select RPAD(1)"));
	REQUIRE_FAIL(con.Query("select RPAD(1, 2)"));
	REQUIRE_FAIL(con.Query("select RPAD('Hello', 10, '')"));
}

TEST_CASE("REPEAT test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test repeat on NULLs
	result = con.Query("select REPEAT(NULL, NULL), REPEAT(NULL, 3), REPEAT('MySQL', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));

	// test repeat on scalars
	result = con.Query("select REPEAT('', 3), REPEAT('MySQL', 3), REPEAT('MotörHead', 2), REPEAT('Hello', -1)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"MySQLMySQLMySQL"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"MotörHeadMotörHead"}));
	REQUIRE(CHECK_COLUMN(result, 3, {""}));

	// test repeat on tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks'), ('', NULL)"));

	result = con.Query("select REPEAT(a, 3) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloHelloHello", "HuLlDHuLlDHuLlD", "MotörHeadMotörHeadMotörHead", ""}));

	result = con.Query("select REPEAT(b, 2) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"WorldWorld", Value(), "RÄcksRÄcks", Value()}));

	result = con.Query("select REPEAT(a, 4) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloHelloHelloHello", "MotörHeadMotörHeadMotörHeadMotörHead"}));

	// test incorrect usage of reverse
	REQUIRE_FAIL(con.Query("select REPEAT()"));
	REQUIRE_FAIL(con.Query("select REPEAT(1)"));
	REQUIRE_FAIL(con.Query("select REPEAT('hello', 'world')"));
	REQUIRE_FAIL(con.Query("select REPEAT('hello', 'world', 3)"));
}

TEST_CASE("REPLACE test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test replace on NULLs
	result = con.Query("select REPLACE('This is the main test string', NULL, 'ALT')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("select REPLACE(NULL, 'main', 'ALT')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("select REPLACE('This is the main test string', 'main', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// test replace on scalars
	result = con.Query("select REPLACE('This is the main test string', 'main', 'ALT')");
	REQUIRE(CHECK_COLUMN(result, 0, {"This is the ALT test string"}));

	result = con.Query("select REPLACE('This is the main test string', 'main', 'larger-main')");
	REQUIRE(CHECK_COLUMN(result, 0, {"This is the larger-main test string"}));

	result = con.Query("select REPLACE('aaaaaaa', 'a', '0123456789')");
	REQUIRE(CHECK_COLUMN(result, 0, {"0123456789012345678901234567890123456789012345678901234567890123456789"}));

	// test replace on tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks'), ('', NULL)"));

	result = con.Query("select REPLACE(a, 'l', '-') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"He--o", "HuL-D", "MotörHead", ""}));

	result = con.Query("select REPLACE(b, 'Ä', '--') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"World", Value(), "R--cks", Value()}));

	result = con.Query("select REPLACE(a, 'H', '') FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"ello", "Motöread"}));

	// test incorrect usage of replace
	REQUIRE_FAIL(con.Query("select REPLACE(1)"));
	REQUIRE_FAIL(con.Query("select REPLACE(1, 2)"));
	REQUIRE_FAIL(con.Query("select REPLACE(1, 2, 3, 4)"));
}

TEST_CASE("REVERSE test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test reverse on scalars
	result = con.Query("select REVERSE(''), REVERSE('Hello'), REVERSE('MotörHead'), REVERSE(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"olleH"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"daeHrötoM"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test reverse on tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks'), ('', NULL)"));

	result = con.Query("select REVERSE(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"olleH", "DlLuH", "daeHrötoM", ""}));

	result = con.Query("select REVERSE(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"dlroW", Value(), "skcÄR", Value()}));

	result = con.Query("select REVERSE(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"olleH", "daeHrötoM"}));

	// test incorrect usage of reverse
	REQUIRE_FAIL(con.Query("select REVERSE()"));
	REQUIRE_FAIL(con.Query("select REVERSE(1, 2)"));
	REQUIRE_FAIL(con.Query("select REVERSE('hello', 'world')"));
}

TEST_CASE("LTRIM/RTRIM test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test ltrim on scalars
	result = con.Query(
	    "select LTRIM(''), LTRIM('Neither'), LTRIM(' Leading'), LTRIM('Trailing   '), LTRIM(' Both '), LTRIM(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Neither"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Leading"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Trailing   "}));
	REQUIRE(CHECK_COLUMN(result, 4, {"Both "}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));

	// test rtrim on scalars
	result = con.Query(
	    "select RTRIM(''), RTRIM('Neither'), RTRIM(' Leading'), RTRIM('Trailing   '), RTRIM(' Both '), RTRIM(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Neither"}));
	REQUIRE(CHECK_COLUMN(result, 2, {" Leading"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Trailing"}));
	REQUIRE(CHECK_COLUMN(result, 4, {" Both"}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));

	// test on tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('', 'Neither'), "
	                          "(' Leading', NULL), (' Both ','Trailing   '), ('', NULL)"));

	result = con.Query("select LTRIM(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"", "Leading", "Both ", ""}));

	result = con.Query("select LTRIM(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"Neither", Value(), "Trailing   ", Value()}));

	result = con.Query("select LTRIM(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"", "Both "}));

	// test rtrim on tables
	result = con.Query("select RTRIM(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"", " Leading", " Both", ""}));

	result = con.Query("select RTRIM(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"Neither", Value(), "Trailing", Value()}));

	result = con.Query("select RTRIM(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"", " Both"}));

	// test incorrect usage of ltrim
	REQUIRE_FAIL(con.Query("select LTRIM()"));
	REQUIRE_FAIL(con.Query("select LTRIM(1, 2)"));
	REQUIRE_FAIL(con.Query("select LTRIM('hello', 'world')"));

	// test incorrect usage of rtrim
	REQUIRE_FAIL(con.Query("select RTRIM()"));
	REQUIRE_FAIL(con.Query("select RTRIM(1, 2)"));
	REQUIRE_FAIL(con.Query("select RTRIM('hello', 'world')"));
}

TEST_CASE("UNICODE test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test on scalars
	result = con.Query("select UNICODE(NULL), UNICODE(''), UNICODE('\x24'), "
	                   "UNICODE('\xC2\xA2'), UNICODE('\xE2\x82\xAC'), UNICODE('\xF0\x90\x8D\x88')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {0x000000}));
	REQUIRE(CHECK_COLUMN(result, 2, {0x000024}));
	REQUIRE(CHECK_COLUMN(result, 3, {0x0000A2}));
	REQUIRE(CHECK_COLUMN(result, 4, {0x0020AC}));
	REQUIRE(CHECK_COLUMN(result, 5, {0x010348}));

	// test on tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES "
	                          "('', 'Zero'), ('\x24', NULL), ('\xC2\xA2','Two'), "
	                          "('\xE2\x82\xAC', NULL), ('\xF0\x90\x8D\x88','Four')"));

	result = con.Query("select UNICODE(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {0x000000, 0x000024, 0x0000A2, 0x0020AC, 0x010348}));

	result = con.Query("select UNICODE(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {90, Value(), 84, Value(), 70}));

	result = con.Query("select UNICODE(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0x000000, 0x0000A2, 0x010348}));

	// test incorrect usage
	REQUIRE_FAIL(con.Query("select UNICODE()"));
	REQUIRE_FAIL(con.Query("select UNICODE(1, 2)"));
}
