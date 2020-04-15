#include "duckdb_miniparquet.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// The tests in this file are taken from https://www.manuelrigger.at/dbms-bugs/
TEST_CASE("Test basic parqet reading", "[parquet]") {
	DuckDB db(nullptr);
	Parquet::Init(db);

	Connection con(db);
	con.EnableQueryVerification();

	SECTION("Exception on missing file") {
		REQUIRE_THROWS(con.Query("SELECT * FROM parquet_scan('does_not_exist')"));
	}

	SECTION("alltypes_plain.snappy.parquet") {
		auto result = con.Query("SELECT * FROM parquet_scan('third_party/miniparquet/test/alltypes_plain.snappy.parquet')");
		result->Print();
	}
}
