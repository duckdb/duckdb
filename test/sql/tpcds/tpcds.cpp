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

//	result = con.Query("SELECT COUNT(*) FROM catalog_returns");
//	REQUIRE(CHECK_COLUMN(result, 0, {144067}));



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
