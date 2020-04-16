#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <iostream>
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-H SF0", "[tpch]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(0, db);
	for (size_t i = 1; i <= 22; i++) {
		REQUIRE_NO_FAIL(con.Query(tpch::get_query(i)));
	}
}

TEST_CASE("Test TPC-H SF0.01", "[tpch]") {
	unique_ptr<QueryResult> result;
	double sf = 0.01;

	// generate the TPC-H data for SF 0.1
	DuckDB db(nullptr);
	Connection con(db);
	tpch::dbgen(sf, db);

	// test all the basic queries
	for (idx_t i = 1; i <= 22; i++) {
		result = con.Query(tpch::get_query(i));
		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
	}
}

TEST_CASE("Test TPC-H SF0.1", "[tpch][.]") {
	unique_ptr<QueryResult> result;
	double sf = 0.1;

	// generate the TPC-H data for SF 0.1
	DuckDB db(nullptr);
	Connection con(db);
	tpch::dbgen(sf, db);

	con.EnableProfiling();

	// check if all the counts are correct
	result = con.Query("SELECT COUNT(*) FROM orders");
	REQUIRE(CHECK_COLUMN(result, 0, {150000}));
	result = con.Query("SELECT COUNT(*) FROM lineitem");
	REQUIRE(CHECK_COLUMN(result, 0, {600572}));
	result = con.Query("SELECT COUNT(*) FROM part");
	REQUIRE(CHECK_COLUMN(result, 0, {20000}));
	result = con.Query("SELECT COUNT(*) FROM partsupp");
	REQUIRE(CHECK_COLUMN(result, 0, {80000}));
	result = con.Query("SELECT COUNT(*) FROM supplier");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	result = con.Query("SELECT COUNT(*) FROM customer");
	REQUIRE(CHECK_COLUMN(result, 0, {15000}));
	result = con.Query("SELECT COUNT(*) FROM nation");
	REQUIRE(CHECK_COLUMN(result, 0, {25}));
	result = con.Query("SELECT COUNT(*) FROM region");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));

	result = con.Query("SELECT * FROM lineitem WHERE l_orderkey <= 1 ORDER BY l_partkey;");
	COMPARE_CSV(result,
	            "1|214|465|4|28|31197.88|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-"
	            "16|NONE|AIR|lites. fluffily even "
	            "de\n1|1564|67|6|32|46897.92|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-"
	            "02-03|DELIVER IN PERSON|MAIL|arefully slyly "
	            "ex\n1|2403|160|5|24|31329.6|0.1|0.04|N|O|1996-03-30|1996-03-14|1996-"
	            "04-01|NONE|FOB| pending foxes. slyly "
	            "re\n1|6370|371|3|8|10210.96|0.1|0.02|N|O|1996-01-29|1996-03-05|1996-"
	            "01-31|TAKE BACK RETURN|REG AIR|riously. regular, express "
	            "dep\n1|6731|732|2|36|58958.28|0.09|0.06|N|O|1996-04-12|1996-02-28|"
	            "1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold "
	            "\n1|15519|785|1|17|24386.67|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-"
	            "03-22|DELIVER IN PERSON|TRUCK|egular courts above the",
	            false);

	result = con.Query("SELECT SUM(l_quantity) FROM lineitem");
	REQUIRE(CHECK_COLUMN(result, 0, {15334802}));
	result = con.Query("SELECT l_quantity % 5 AS f, COUNT(*) FROM lineitem "
	                   "GROUP BY f ORDER BY f;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {119525, 120331, 120426, 119986, 120304}));
	result = con.Query("SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM "
	                   "lineitem GROUP BY l_returnflag;");
	REQUIRE(CHECK_COLUMN(result, 0, {"A", "N", "R"}));
	REQUIRE(CHECK_COLUMN(result, 1, {3774200, 7775079, 3785523}));
	REQUIRE(CHECK_COLUMN(result, 2, {147790, 304481, 148301}));
	result = con.Query("SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM lineitem WHERE "
	                   "l_shipdate <= cast('1998-09-02' as date) GROUP BY l_returnflag;");
	REQUIRE(CHECK_COLUMN(result, 0, {"A", "N", "R"}));
	REQUIRE(CHECK_COLUMN(result, 1, {3774200, 7554554, 3785523}));
	REQUIRE(CHECK_COLUMN(result, 2, {147790, 295765, 148301}));

	// this would really hurt without pushdown
	result = con.Query("SELECT count(*) FROM lineitem JOIN orders ON "
	                   "lineitem.l_orderkey=orders.o_orderkey WHERE "
	                   "o_orderstatus='X' AND l_tax > 50");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// test all the basic queries
	for (idx_t i = 1; i <= 22; i++) {
		result = con.Query(tpch::get_query(i));
		COMPARE_CSV(result, tpch::get_answer(sf, i), true);
	}
}
