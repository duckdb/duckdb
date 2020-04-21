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

	// test upper/lower on scalar values
	result = con.Query("select UPPER(''), UPPER('hello'), UPPER('MotörHead'), UPPER(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HELLO"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"MOTöRHEAD"}));
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
	REQUIRE(CHECK_COLUMN(result, 2, {"MOTöRHEAD"}));
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
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "HULLD", "MOTöRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HELLO", "HULLD", "MOTöRHEAD"}));

	result = con.Query("select LOWER(a), LCASE(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hulld", "motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hulld", "motörhead"}));

	result = con.Query("select LOWER(b), LCASE(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"world", Value(), "rÄcks"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"world", Value(), "rÄcks"}));

	// test with selection vector
	result = con.Query("select UPPER(a), LOWER(a), UCASE(a), LCASE(a) FROM strings WHERE b IS NOT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "MOTöRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "motörhead"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"HELLO", "MOTöRHEAD"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello", "motörhead"}));
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
	result = con.Query("select LTRIM(''), LTRIM('Neither'), LTRIM(' Leading'), LTRIM('Trailing   '), LTRIM(' Both '), LTRIM(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Neither"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"Leading"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Trailing   "}));
	REQUIRE(CHECK_COLUMN(result, 4, {"Both "}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));

	// test rtrim on scalars
	result = con.Query("select RTRIM(''), RTRIM('Neither'), RTRIM(' Leading'), RTRIM('Trailing   '), RTRIM(' Both '), RTRIM(NULL)");
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
