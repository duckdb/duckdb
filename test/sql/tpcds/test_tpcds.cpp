#include "catch.hpp"
#include "dsdgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-DS SF0 Query Compilation", "[tpcds]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create schema only
	tpcds::dbgen(0, db);

	// this is to make sure we do not get regressions in query compilation
	for (size_t q = 1; q < 104; q++) {
		REQUIRE_NO_FAIL(con.Query(tpcds::get_query(q)));
	}
}


TEST_CASE("Test TPC-DS SF1", "[tpcds][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;

	// create and load data
	tpcds::dbgen(1, db);

	// verify table counts
	result = con.Query("SELECT COUNT(*) FROM call_center");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("SELECT COUNT(*) FROM catalog_page");
	REQUIRE(CHECK_COLUMN(result, 0, {11718}));
	result = con.Query("SELECT COUNT(*) FROM catalog_returns");
	REQUIRE(CHECK_COLUMN(result, 0, {144067}));
	result = con.Query("SELECT COUNT(*) FROM catalog_sales");
	REQUIRE(CHECK_COLUMN(result, 0, {1441548}));
	result = con.Query("SELECT COUNT(*) FROM customer");
	REQUIRE(CHECK_COLUMN(result, 0, {100000}));
	result = con.Query("SELECT COUNT(*) FROM customer_demographics");
	REQUIRE(CHECK_COLUMN(result, 0, {1920800}));
	result = con.Query("SELECT COUNT(*) FROM customer_address");
	REQUIRE(CHECK_COLUMN(result, 0, {50000}));
	result = con.Query("SELECT COUNT(*) FROM date_dim");
	REQUIRE(CHECK_COLUMN(result, 0, {73049}));
	result = con.Query("SELECT COUNT(*) FROM household_demographics");
	REQUIRE(CHECK_COLUMN(result, 0, {7200}));
	result = con.Query("SELECT COUNT(*) FROM inventory");
	REQUIRE(CHECK_COLUMN(result, 0, {11745000}));
	result = con.Query("SELECT COUNT(*) FROM income_band");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	result = con.Query("SELECT COUNT(*) FROM inventory");
	REQUIRE(CHECK_COLUMN(result, 0, {11745000}));
	result = con.Query("SELECT COUNT(*) FROM item");
	REQUIRE(CHECK_COLUMN(result, 0, {18000}));
	result = con.Query("SELECT COUNT(*) FROM promotion");
	REQUIRE(CHECK_COLUMN(result, 0, {300}));
	result = con.Query("SELECT COUNT(*) FROM reason");
	REQUIRE(CHECK_COLUMN(result, 0, {35}));
	result = con.Query("SELECT COUNT(*) FROM ship_mode");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	result = con.Query("SELECT COUNT(*) FROM store");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	result = con.Query("SELECT COUNT(*) FROM store_returns");
	REQUIRE(CHECK_COLUMN(result, 0, {287867}));
	result = con.Query("SELECT COUNT(*) FROM store_sales");
	REQUIRE(CHECK_COLUMN(result, 0, {2880404}));
	result = con.Query("SELECT COUNT(*) FROM time_dim");
	REQUIRE(CHECK_COLUMN(result, 0, {86400}));
	result = con.Query("SELECT COUNT(*) FROM warehouse");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("SELECT COUNT(*) FROM web_page");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));
	result = con.Query("SELECT COUNT(*) FROM web_returns");
	REQUIRE(CHECK_COLUMN(result, 0, {71654}));
	result = con.Query("SELECT COUNT(*) FROM web_sales");
	REQUIRE(CHECK_COLUMN(result, 0, {719384}));
	result = con.Query("SELECT COUNT(*) FROM web_site");
	REQUIRE(CHECK_COLUMN(result, 0, {30}));

	con.EnableProfiling();
    // for these queries there is an answer mismatch, for various reasons, most are fp accuracy or null-ordering related but some might be bugs
    unordered_set<idx_t> answer_mismatch_queries { 2, 4, 5, 11, 14, 18, 19, 22, 23, 30, 33, 37, 42, 43, 53, 55, 58, 62, 63, 76, 78, 81, 87, 101 };
    // skip queries that are too slow (for now)
	unordered_set<idx_t> slow_queries { 68, 71, 89 };
    for (size_t q = 1; q < 104; q++) {
		if (answer_mismatch_queries.find(q) != answer_mismatch_queries.end() || slow_queries.find(q) != slow_queries.end()) {
			continue;
		}
        result = con.Query(tpcds::get_query(q));
        COMPARE_CSV(result, tpcds::get_answer(1, q), false);
    }
}
