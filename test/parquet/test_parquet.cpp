#include "duckdb_miniparquet.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic parquet reading", "[parquet]") {
	DuckDB db(nullptr);
	Parquet::Init(db);

	Connection con(db);
	con.EnableQueryVerification();

	SECTION("Exception on missing file") {
		REQUIRE_THROWS(con.Query("SELECT * FROM parquet_scan('does_not_exist')"));
	}

	SECTION("alltypes_plain.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('third_party/miniparquet/test/alltypes_plain.parquet')");
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
		auto result =
		    con.Query("SELECT * FROM parquet_scan('third_party/miniparquet/test/alltypes_plain.snappy.parquet')");
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
		auto result =
		    con.Query("SELECT * FROM parquet_scan('third_party/miniparquet/test/alltypes_dictionary.parquet')");

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

	SECTION("userdata1.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('third_party/miniparquet/test/userdata1.parquet')");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	}
}

TEST_CASE("Test TPCH SF1 from parquet file", "[parquet][.]") {
	DuckDB db(nullptr);
	Parquet::Init(db);
	Connection con(db);

	con.Query("CREATE VIEW lineitem AS SELECT * FROM "
	          "parquet_scan('third_party/miniparquet/test/lineitemsf1.snappy.parquet')");
	auto result = con.Query(tpch::get_query(1));
	COMPARE_CSV(result, tpch::get_answer(1, 1), true);
}

TEST_CASE("XXX", "[parquet][.]") {
	DuckDB db(nullptr);
	Parquet::Init(db);
	Connection con(db);

	auto result = con.Query(
	    "SELECT first_name, last_name FROM parquet_scan('third_party/miniparquet/test/userdata1.parquet') limit 10");
	result->Print();

	con.Query("CREATE VIEW userdata AS SELECT * FROM parquet_scan('third_party/miniparquet/test/userdata1.parquet')");
	result = con.Query("SELECT first_name, last_name FROM userdata limit 10");
	result->Print();
}
