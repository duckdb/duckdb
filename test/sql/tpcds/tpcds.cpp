#include "catch.hpp"
#include "dsdgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-DS SF1", "[tpcds][.]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

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
	//	REQUIRE(CHECK_COLUMN(result, 0, {1441548}));
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
	// REQUIRE(CHECK_COLUMN(result, 0, {287514}));
	result = con.Query("SELECT COUNT(*) FROM store_sales");
	// REQUIRE(CHECK_COLUMN(result, 0, {2880404}));
	result = con.Query("SELECT COUNT(*) FROM time_dim");
	REQUIRE(CHECK_COLUMN(result, 0, {86400}));
	result = con.Query("SELECT COUNT(*) FROM warehouse");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("SELECT COUNT(*) FROM web_page");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));
	result = con.Query("SELECT COUNT(*) FROM web_returns");
	// REQUIRE(CHECK_COLUMN(result, 0, {71763}));
	result = con.Query("SELECT COUNT(*) FROM web_sales");
	// REQUIRE(CHECK_COLUMN(result, 0, {719384}));
	result = con.Query("SELECT COUNT(*) FROM web_site");
	REQUIRE(CHECK_COLUMN(result, 0, {30}));

	/*


	    +------------------------+----------+
	    | tbl                    | cnt      |
	    +========================+==========+
	    | call_center            |        6 |
	    | catalog_page           |    11718 |
	    | catalog_returns        |   144067 |
	    | catalog_sales          |  1441548 |
	    | customer               |   100000 |
	    | customer_address       |    50000 |
	    | customer_demographics  |  1920800 |
	    | date_dim               |    73049 |
	    | dbgen_version          |        1 |
	    | household_demographics |     7200 |
	    | income_band            |       20 |
	    | inventory              | 11745000 |
	    | item                   |    18000 |
	    | promotion              |      300 |
	    | reason                 |       35 |
	    | ship_mode              |       20 |
	    | store                  |       12 |
	    | store_returns          |   287514 |
	    | store_sales            |  2880404 |
	    | time_dim               |    86400 |
	    | warehouse              |        5 |
	    | web_page               |       60 |
	    | web_returns            |    71763 |
	    | web_sales              |   719384 |
	    | web_site               |       30 |
	    +------------------------+----------+
	    */

	//	// run queries, these work already
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q01]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q06]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q07]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q10]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q12]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q15]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q19]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q20]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q21]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q25]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q27]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q29]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q33]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q35]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q37]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q40]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q42]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q43]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q45]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q48]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q50]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q52]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q53]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q55]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q61]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q62]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q63]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q65]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q73]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q79]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q82]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q85]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q88]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q89]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q90]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q91]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q92]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q93]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q96]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q98]);
	//	con.Query(TPCDS_QUERIES[TPCDS_QUERY_ID::Q99]);
	//
	//	// TODO result verification
}
