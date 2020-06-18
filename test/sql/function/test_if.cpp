#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("IF test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT IF(true, 1, 10), IF(false, 1, 10), IF(NULL, 1, 10)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	REQUIRE(CHECK_COLUMN(result, 2, {10}));

	result = con.Query("SELECT IF(true, 20, 2000), IF(false, 20, 2000), IF(NULL, 20, 2000)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));
	REQUIRE(CHECK_COLUMN(result, 2, {2000}));

	result = con.Query("SELECT IF(true, 20.5, 2000), IF(false, 20, 2000.5), IF(NULL, 20, 2000.5)");
	REQUIRE(CHECK_COLUMN(result, 0, {20.5}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000.5}));

	result = con.Query("SELECT IF(true, '2020-05-05'::date, '1996-11-05 10:11:56'::timestamp), "
	                   "IF(false, '2020-05-05'::date, '1996-11-05 10:11:56'::timestamp), "
	                   "IF(NULL, '2020-05-05'::date, '1996-11-05 10:11:56'::timestamp)");
	REQUIRE(CHECK_COLUMN(result, 0, {Timestamp::FromString("2020-05-05 00:00:00")}));
	REQUIRE(CHECK_COLUMN(result, 1, {Timestamp::FromString("1996-11-05 10:11:56")}));
	REQUIRE(CHECK_COLUMN(result, 2, {Timestamp::FromString("1996-11-05 10:11:56")}));

	result = con.Query("SELECT IF(true, 'true', 'false'), IF(false, 'true', 'false'), IF(NULL, 'true', 'false')");
	REQUIRE(CHECK_COLUMN(result, 0, {"true"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"false"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"false"}));
}

TEST_CASE("IFNULL test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT IFNULL(NULL, NULL), IFNULL(NULL, 10), IFNULL(1, 10)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));

	result = con.Query("SELECT IFNULL(NULL, 2000), IFNULL(20.5, 2000)");
	REQUIRE(CHECK_COLUMN(result, 0, {2000.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {20.5}));

	result = con.Query("SELECT IFNULL(NULL, '1996-11-05 10:11:56'::timestamp), "
	                   "IFNULL('2020-05-05'::date, '1996-11-05 10:11:56'::timestamp)");
	REQUIRE(CHECK_COLUMN(result, 0, {Timestamp::FromString("1996-11-05 10:11:56")}));
	REQUIRE(CHECK_COLUMN(result, 1, {Timestamp::FromString("2020-05-05 00:00:00")}));

	result = con.Query("SELECT IFNULL(NULL, 'not NULL'), IFNULL('NULL', 'not NULL')");
	REQUIRE(CHECK_COLUMN(result, 0, {"not NULL"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"NULL"}));
}
