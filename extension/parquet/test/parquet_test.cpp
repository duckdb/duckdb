#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "parquet-extension.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/timestamp.hpp"
//#include "dbgen.hpp"

using namespace duckdb;

TEST_CASE("Test basic parquet reading", "[parquet]") {
	DuckDB db(nullptr);
	db.LoadExtension<ParquetExtension>();

	Connection con(db);

	con.EnableQueryVerification();

	SECTION("Exception on missing file") {
		REQUIRE_THROWS(con.Query("SELECT * FROM parquet_scan('does_not_exist')"));
	}

	SECTION("alltypes_plain.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_plain.parquet')");
		REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 6, 7, 2, 3, 0, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {true, false, true, false, true, false, true, false}));
		REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 0, 1, 0, 1, 0, 1}));
		REQUIRE(CHECK_COLUMN(result, 3, {0, 1, 0, 1, 0, 1, 0, 1}));
		REQUIRE(CHECK_COLUMN(result, 4, {0, 1, 0, 1, 0, 1, 0, 1}));
		REQUIRE(CHECK_COLUMN(result, 5, {0, 10, 0, 10, 0, 10, 0, 10}));
		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1}));
		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1}));
		REQUIRE(CHECK_COLUMN(
		    result, 8,
		    {"03/01/09", "03/01/09", "04/01/09", "04/01/09", "02/01/09", "02/01/09", "01/01/09", "01/01/09"}));
		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1", "0", "1", "0", "1", "0", "1"}));

		REQUIRE(CHECK_COLUMN(result, 10,
		                     {Value::BIGINT(Timestamp::FromString("2009-03-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-03-01 00:01:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:01:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-02-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-02-01 00:01:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:01:00"))}));
	}

	SECTION("alltypes_plain.snappy.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_plain.snappy.parquet')");
		REQUIRE(CHECK_COLUMN(result, 0, {6, 7}));
		REQUIRE(CHECK_COLUMN(result, 1, {true, false}));
		REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 3, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 4, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 5, {0, 10}));
		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1}));
		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1}));
		REQUIRE(CHECK_COLUMN(result, 8, {"04/01/09", "04/01/09"}));
		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1"}));
		REQUIRE(CHECK_COLUMN(result, 10,
		                     {Value::BIGINT(Timestamp::FromString("2009-04-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:01:00"))}));
	}

	SECTION("alltypes_dictionary.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_dictionary.parquet')");

		REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {true, false}));
		REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 3, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 4, {0, 1}));
		REQUIRE(CHECK_COLUMN(result, 5, {0, 10}));
		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1}));
		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1}));
		REQUIRE(CHECK_COLUMN(result, 8, {"01/01/09", "01/01/09"}));
		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1"}));
		REQUIRE(CHECK_COLUMN(result, 10,
		                     {Value::BIGINT(Timestamp::FromString("2009-01-01 00:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:01:00"))}));
	}

	// this file was created with spark using the data-types.py script
	SECTION("data-types.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/data-types.parquet')");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 42, -127, 127, Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, -32767, 32767, Value()}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), 44, -2147483647, 2147483647, Value()}));
		REQUIRE(CHECK_COLUMN(
		    result, 3,
		    {Value(), 45, Value::BIGINT(-9223372036854775807), Value::BIGINT(9223372036854775807), Value()}));
		REQUIRE(CHECK_COLUMN(result, 4, {Value(), 4.6, -4.6, Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 5, {Value(), 4.7, -4.7, Value(), Value()}));
		// REQUIRE(CHECK_COLUMN(result, 6, {Value(), 4.8, -128, 127, Value()})); // decimal :/
		REQUIRE(CHECK_COLUMN(result, 7, {Value(), "49", Value(), Value(), Value()}));
		// REQUIRE(CHECK_COLUMN(result, 8, {Value(), "50", -128, 127, Value()})); // blob :/
		REQUIRE(CHECK_COLUMN(result, 9, {Value(), true, false, Value(), Value()}));
		REQUIRE(CHECK_COLUMN(
		    result, 10,
		    {Value(), Value::BIGINT(Timestamp::FromString("2019-11-26 20:11:42.501")), Value(), Value(), Value()}));
		// REQUIRE(CHECK_COLUMN(result, 11, {Value(), false, -128, 127, Value()})); // date :/
	}

	SECTION("userdata1.parquet") {

		auto result = con.Query("SELECT COUNT(*) FROM  parquet_scan('extension/parquet/test/userdata1.parquet')");

		REQUIRE(CHECK_COLUMN(result, 0, {1000}));

		con.Query("CREATE VIEW userdata1 AS SELECT * FROM parquet_scan('extension/parquet/test/userdata1.parquet')");

		result = con.Query("SELECT COUNT(*) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));

		con.DisableQueryVerification(); // TOO slow

		result = con.Query("SELECT COUNT(registration_dttm), COUNT(id), COUNT(first_name), COUNT(last_name), "
		                   "COUNT(email), COUNT(gender), COUNT(ip_address), COUNT(cc), COUNT(country), "
		                   "COUNT(birthdate), COUNT(salary), COUNT(title), COUNT(comments) FROM userdata1");

		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		REQUIRE(CHECK_COLUMN(result, 1, {1000}));
		REQUIRE(CHECK_COLUMN(result, 2, {1000}));
		REQUIRE(CHECK_COLUMN(result, 3, {1000}));
		REQUIRE(CHECK_COLUMN(result, 4, {1000}));
		REQUIRE(CHECK_COLUMN(result, 5, {1000}));
		REQUIRE(CHECK_COLUMN(result, 6, {1000}));
		REQUIRE(CHECK_COLUMN(result, 7, {1000}));
		REQUIRE(CHECK_COLUMN(result, 8, {1000}));
		REQUIRE(CHECK_COLUMN(result, 9, {1000}));
		REQUIRE(CHECK_COLUMN(result, 10, {932}));
		REQUIRE(CHECK_COLUMN(result, 11, {1000}));
		REQUIRE(CHECK_COLUMN(result, 12, {994}));

		result = con.Query("SELECT MIN(registration_dttm), MAX(registration_dttm) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2016-02-03 00:01:00"))}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(Timestamp::FromString("2016-02-03 23:59:55"))}));

		result = con.Query("SELECT MIN(id), MAX(id) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {1000}));

		result = con.Query(
		    "SELECT FIRST(id) OVER w, LAST(id) OVER w FROM userdata1 WINDOW w AS (ORDER BY id RANGE BETWEEN UNBOUNDED "
		    "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {1000}));

		result = con.Query("SELECT MIN(first_name), MAX(first_name) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Willie"}));

		result = con.Query("SELECT FIRST(first_name) OVER w, LAST(first_name) OVER w FROM userdata1 WINDOW w AS (ORDER "
		                   "BY id RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Amanda"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Julie"}));

		result = con.Query("SELECT MIN(last_name), MAX(last_name) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Adams"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Young"}));

		result = con.Query("SELECT FIRST(last_name) OVER w, LAST(last_name) OVER w FROM userdata1 WINDOW w AS (ORDER "
		                   "BY id RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Jordan"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Meyer"}));

		result = con.Query("SELECT MIN(email), MAX(email) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"wweaver2r@google.de"}));

		result = con.Query("SELECT FIRST(email) OVER w, LAST(email) OVER w FROM userdata1 WINDOW w AS (ORDER "
		                   "BY id RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"ajordan0@com.com"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"jmeyerrr@flavors.me"}));

		result = con.Query("SELECT MIN(gender), MAX(gender) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Male"}));

		result = con.Query("SELECT FIRST(gender) OVER w, LAST(gender) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Female"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Female"}));

		result = con.Query("SELECT MIN(ip_address), MAX(ip_address) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {"0.14.221.162"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"99.159.168.233"}));

		result = con.Query(
		    "SELECT FIRST(ip_address) OVER w, LAST(ip_address) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		    "RANGE BETWEEN UNBOUNDED "
		    "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"1.197.201.2"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"217.1.147.132"}));

		result = con.Query("SELECT MIN(cc), MAX(cc) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"67718647521473678"}));

		result = con.Query("SELECT FIRST(cc) OVER w, LAST(cc) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"6759521864920116"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"374288099198540"}));

		result = con.Query("SELECT MIN(country), MAX(country) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {"\"Bonaire"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Zimbabwe"}));

		result = con.Query("SELECT FIRST(country) OVER w, LAST(country) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Indonesia"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"China"}));

		result = con.Query("SELECT MIN(birthdate), MAX(birthdate) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"9/9/1981"}));

		result =
		    con.Query("SELECT FIRST(birthdate) OVER w, LAST(birthdate) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		              "RANGE BETWEEN UNBOUNDED "
		              "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"3/8/1971"}));
		REQUIRE(CHECK_COLUMN(result, 1, {""}));

		result = con.Query("SELECT MIN(salary), MAX(salary) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {12380.490000}));
		REQUIRE(CHECK_COLUMN(result, 1, {286592.990000}));

		result = con.Query("SELECT FIRST(salary) OVER w, LAST(salary) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {49756.530000}));
		REQUIRE(CHECK_COLUMN(result, 1, {222561.130000}));

		result = con.Query("SELECT MIN(title), MAX(title) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Web Developer IV"}));

		result = con.Query("SELECT FIRST(title) OVER w, LAST(title) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Internal Auditor"}));
		REQUIRE(CHECK_COLUMN(result, 1, {""}));

		result = con.Query("SELECT MIN(comments), MAX(comments) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"𠜎𠜱𠝹𠱓𠱸𠲖𠳏"}));

		result = con.Query("SELECT FIRST(comments) OVER w, LAST(comments) OVER w FROM userdata1 WINDOW w AS (ORDER BY "
		                   "id RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"1E+02"}));
		REQUIRE(CHECK_COLUMN(result, 1, {""}));
	}
}
//
// TEST_CASE("Test TPCH SF1 from parquet file", "[parquet][.]") {
//	DuckDB db(nullptr);
//	db.LoadExtension<ParquetExtension>();
//	Connection con(db);
//
//	auto result = con.Query("SELECT * FROM "
//	                        "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet') limit 10");
//	result->Print();
//
//	con.Query("CREATE VIEW lineitem AS SELECT * FROM "
//	          "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet')");
//	result = con.Query(tpch::get_query(1));
//	COMPARE_CSV(result, tpch::get_answer(1, 1), true);
//}
