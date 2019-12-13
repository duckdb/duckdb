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
	unordered_set<size_t> missing_queries = {55, 101};
	for (size_t q = 1; q < 104; q++) {
		if (missing_queries.count(q) != 0) {
			continue;
		}
		REQUIRE_NO_FAIL(con.Query(tpcds::get_query(q)));
	}
}

TEST_CASE("Test TPC-DS SF0.1 Query Execution", "[tpcds][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;

	tpcds::dbgen(0.01, db);
	con.EnableProfiling();

	unordered_set<size_t> missing_queries = {14, 40, 51, 55, 68, 76, 86, 89, 101};
	for (size_t q = 1; q < 104; q++) {
		if (missing_queries.count(q) != 0) {
			continue;
		}
		REQUIRE_NO_FAIL(con.Query(tpcds::get_query(q)));
	}
}

TEST_CASE("Test TPC-DS SF1", "[tpcds][.]") {
	return;
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
	// TODO: this count is slightly off, why?
	//	REQUIRE(CHECK_COLUMN(result, 0, {287514}));
	result = con.Query("SELECT COUNT(*) FROM store_sales");
	REQUIRE(CHECK_COLUMN(result, 0, {2880404}));
	result = con.Query("SELECT COUNT(*) FROM time_dim");
	REQUIRE(CHECK_COLUMN(result, 0, {86400}));
	result = con.Query("SELECT COUNT(*) FROM warehouse");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("SELECT COUNT(*) FROM web_page");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));
	result = con.Query("SELECT COUNT(*) FROM web_returns");
	// TODO: this count is slightly off, why?
	// REQUIRE(CHECK_COLUMN(result, 0, {71763}));
	result = con.Query("SELECT COUNT(*) FROM web_sales");
	REQUIRE(CHECK_COLUMN(result, 0, {719384}));
	result = con.Query("SELECT COUNT(*) FROM web_site");
	REQUIRE(CHECK_COLUMN(result, 0, {30}));

	con.EnableProfiling();

	//	// run queries, these work already
	// con.Query(tpcds::get_query(6))->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q06])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q07])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q10])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q12])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q15])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q19])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q20])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q21])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q25])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q27])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q29])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q33])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q35])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q37])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q40])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q42])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q43])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q45])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q48])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q50])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q52])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q53])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q55])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q61])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q62])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q63])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q65])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q73])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q79])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q82])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q85])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q88])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q89])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q90])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q91])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q92])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q93])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q96])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q98])->Print();
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q99])->Print();
	//
	//	// TODO result verification
}
