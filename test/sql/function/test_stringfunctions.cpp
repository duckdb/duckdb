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
	REQUIRE(CHECK_COLUMN(result, 0, {"PREFIXWorld", Value(), "PREFIXRÄcks"}));

	result = con.Query("select CONCAT(a, b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorld", Value(), "MotörHeadRÄcks"}));

	result = con.Query("select CONCAT(a, b, 'SUFFIX') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorldSUFFIX", Value(), "MotörHeadRÄcksSUFFIX"}));

	result = con.Query("select CONCAT(a, b, a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HelloWorldHello", Value(), "MotörHeadRÄcksMotörHead"}));

	result = con.Query("select CONCAT('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')");
	REQUIRE(CHECK_COLUMN(result, 0, {"1234567890"}));
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

	// filters
	result = con.Query("select CONCAT_WS(',', a, 'SUFFIX') FROM strings WHERE a != 'Hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"HuLlD,SUFFIX", "MotörHead,SUFFIX"}));
}

TEST_CASE("UPPER/LOWER test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a STRING, b STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World'), "
	                          "('HuLlD', NULL), ('MotörHead','RÄcks')"));

	result = con.Query("select UPPER(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "HULLD", "MOTöRHEAD"}));

	result = con.Query("select LOWER(a) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hulld", "motörhead"}));

	result = con.Query("select LOWER(b) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"world", Value(), "rÄcks"}));
}
