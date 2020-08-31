// #include "catch.hpp"
// #include "tpch-extension.hpp"
// #include "test_helpers.hpp"

// #include <chrono>
// #include <iostream>
// #include "duckdb/common/string_util.hpp"

// using namespace duckdb;
// using namespace std;


// TEST_CASE("Test TPC-H SF0.01", "[tpch]") {
// 	unique_ptr<QueryResult> result;
// 	double sf = 0.01;

// 	// generate the TPC-H data for SF 0.1
// 	DuckDB db(nullptr);
// 	Connection con(db);
// 	tpch::dbgen(sf, db);

// 	// test all the basic queries
// 	for (idx_t i = 1; i <= 22; i++) {
// 		result = con.Query(tpch::get_query(i));
// 		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
// 	}
// }

// TEST_CASE("Test Parallel TPC-H SF0.01", "[tpch]") {
// 	unique_ptr<QueryResult> result;
// 	double sf = 0.01;

// 	// generate the TPC-H data for SF 0.01
// 	DuckDB db(nullptr);
// 	Connection con(db);

// 	// con.Query("PRAGMA enable_profiling");

// 	// initialize background threads
// 	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));
// 	if (STANDARD_VECTOR_SIZE >= 512) {
// 		// this just takes too long on vsize = 2, because lineitem gets split into 30K tasks
// 		con.ForceParallelism();
// 	}

// 	tpch::dbgen(sf, db);

// 	// test all the basic queries
// 	for (idx_t i = 1; i <= 22; i++) {
// 		result = con.Query(tpch::get_query(i));
// 		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
// 	}
// }

// TEST_CASE("Test Parallel TPC-H SF0.1", "[tpch][.]") {
// 	unique_ptr<QueryResult> result;
// 	double sf = 0.1;

// 	// generate the TPC-H data for SF 0.1
// 	DuckDB db(nullptr);
// 	Connection con(db);

// 	con.DisableProfiling();

// 	// initialize background threads
// 	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));

// 	tpch::dbgen(sf, db);

// 	// test all the basic queries
// 	for (idx_t i = 1; i <= 22; i++) {
// 		result = con.Query(tpch::get_query(i));
// 		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
// 	}
// }

// TEST_CASE("Test TPC-H SF0.1", "[tpch][.]") {
// 	unique_ptr<QueryResult> result;
// 	double sf = 0.1;

// 	// generate the TPC-H data for SF 0.1
// 	DuckDB db(nullptr);
// 	Connection con(db);
// 	tpch::dbgen(sf, db);

// 	con.EnableProfiling();

// 	// check if all the counts are correct
// 	result = con.Query("SELECT COUNT(*) FROM orders");
// 	REQUIRE(CHECK_COLUMN(result, 0, {150000}));
// 	result = con.Query("SELECT COUNT(*) FROM lineitem");
// 	REQUIRE(CHECK_COLUMN(result, 0, {600572}));
// 	result = con.Query("SELECT COUNT(*) FROM part");
// 	REQUIRE(CHECK_COLUMN(result, 0, {20000}));
// 	result = con.Query("SELECT COUNT(*) FROM partsupp");
// 	REQUIRE(CHECK_COLUMN(result, 0, {80000}));
// 	result = con.Query("SELECT COUNT(*) FROM supplier");
// 	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
// 	result = con.Query("SELECT COUNT(*) FROM customer");
// 	REQUIRE(CHECK_COLUMN(result, 0, {15000}));
// 	result = con.Query("SELECT COUNT(*) FROM nation");
// 	REQUIRE(CHECK_COLUMN(result, 0, {25}));
// 	result = con.Query("SELECT COUNT(*) FROM region");
// 	REQUIRE(CHECK_COLUMN(result, 0, {5}));

// 	result = con.Query("SELECT SUM(l_quantity) FROM lineitem");
// 	REQUIRE(CHECK_COLUMN(result, 0, {15334802}));
// 	result = con.Query("SELECT l_quantity % 5 AS f, COUNT(*) FROM lineitem "
// 	                   "GROUP BY f ORDER BY f;");
// 	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {119525, 120331, 120426, 119986, 120304}));
// 	result = con.Query("SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM "
// 	                   "lineitem GROUP BY l_returnflag;");
// 	REQUIRE(CHECK_COLUMN(result, 0, {"A", "N", "R"}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {3774200, 7775079, 3785523}));
// 	REQUIRE(CHECK_COLUMN(result, 2, {147790, 304481, 148301}));
// 	result = con.Query("SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM lineitem WHERE "
// 	                   "l_shipdate <= cast('1998-09-02' as date) GROUP BY l_returnflag;");
// 	REQUIRE(CHECK_COLUMN(result, 0, {"A", "N", "R"}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {3774200, 7554554, 3785523}));
// 	REQUIRE(CHECK_COLUMN(result, 2, {147790, 295765, 148301}));

// 	// this would really hurt without pushdown
// 	result = con.Query("SELECT count(*) FROM lineitem JOIN orders ON "
// 	                   "lineitem.l_orderkey=orders.o_orderkey WHERE "
// 	                   "o_orderstatus='X' AND l_tax > 50");
// 	REQUIRE(CHECK_COLUMN(result, 0, {0}));

// 	// test all the basic queries
// 	for (idx_t i = 1; i <= 22; i++) {
// 		result = con.Query(tpch::get_query(i));
// 		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
// 	}
// }
