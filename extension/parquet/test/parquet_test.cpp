#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "parquet-extension.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "dbgen.hpp"

using namespace duckdb;

TEST_CASE("Test basic parquet reading", "[parquet]") {
	DuckDB db(nullptr);
	db.LoadExtension<ParquetExtension>();

	Connection con(db);

	con.EnableQueryVerification();

	SECTION("userdata1.parquet") {

		auto result = con.Query("SELECT COUNT(*) FROM parquet_scan('extension/parquet/test/userdata1.parquet')");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));

		con.Query(
		    "CREATE VIEW userdata1 AS SELECT * FROM parquet_scan('extension/parquet/test/userdata1.parquet')");

		result = con.Query("SELECT COUNT(*) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));

		result = con.Query("SELECT COUNT(registration_dttm), COUNT(id), COUNT(first_name), COUNT(last_name), "
		                   "COUNT(email), COUNT(gender), COUNT(ip_address), COUNT(cc), COUNT(country), "
		                   "COUNT(birthdate), COUNT(salary), COUNT(title), COUNT(comments) FROM userdata1");

		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		REQUIRE(CHECK_COLUMN(result, 1, {1000}));
		REQUIRE(CHECK_COLUMN(result, 2, {1000}));
		REQUIRE(CHECK_COLUMN(result, 3, {1000}));
		REQUIRE(CHECK_COLUMN(result, 4, {0}));
		REQUIRE(CHECK_COLUMN(result, 5, {1000}));
		REQUIRE(CHECK_COLUMN(result, 6, {0}));
		REQUIRE(CHECK_COLUMN(result, 7, {0}));
		REQUIRE(CHECK_COLUMN(result, 8, {1000}));
		REQUIRE(CHECK_COLUMN(result, 9, {0}));
		REQUIRE(CHECK_COLUMN(result, 10, {1000}));
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
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

		result = con.Query("SELECT MIN(gender), MAX(gender) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Male"}));

		result = con.Query("SELECT FIRST(gender) OVER w, LAST(gender) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Female"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Female"}));

		result = con.Query("SELECT MIN(ip_address), MAX(ip_address) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

		result = con.Query("SELECT MIN(cc), MAX(cc) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

		result = con.Query("SELECT MIN(country), MAX(country) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {"\"Bonaire"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"Zimbabwe"}));

		result = con.Query("SELECT FIRST(country) OVER w, LAST(country) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
		                   "RANGE BETWEEN UNBOUNDED "
		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
		REQUIRE(CHECK_COLUMN(result, 0, {"Indonesia"}));
		REQUIRE(CHECK_COLUMN(result, 1, {"China"}));

		result = con.Query("SELECT MIN(birthdate), MAX(birthdate) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

		result = con.Query("SELECT MIN(salary), MAX(salary) FROM userdata1");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));

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
		REQUIRE(CHECK_COLUMN(result, 1, {"1E+02"}));
	}
}

TEST_CASE("Test TPCH SF1 from parquet file", "[parquet][.]") {
	DuckDB db(nullptr);
	db.LoadExtension<ParquetExtension>();
	Connection con(db);

	auto result = con.Query("SELECT * FROM "
	                        "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet') limit 10");
	result->Print();

	con.Query("CREATE VIEW lineitem AS SELECT * FROM "
	          "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet')");
	result = con.Query(tpch::get_query(1));
	COMPARE_CSV(result, tpch::get_answer(1, 1), true);
}
