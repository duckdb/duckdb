#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("MonetDB Test: leftjoin.Bug-3981.sql", "[monetdb]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT * FROM ( SELECT 'apple' as fruit UNION ALL SELECT 'banana' ) a JOIN ( SELECT 'apple' as fruit UNION ALL SELECT 'banana' ) b ON a.fruit=b.fruit LEFT JOIN ( SELECT 1 as isyellow ) c ON b.fruit='banana';");
	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1}));
}
