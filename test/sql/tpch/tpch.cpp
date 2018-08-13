
#include "catch.hpp"
#include "dbgen.hpp"

#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("[SLOW] Test TPC-H SF0.1", "[tpch]") {
	unique_ptr<DuckDBResult> result;
	double sf = 0.1;

	// generate the TPC-H data for SF 0.1
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	// REQUIRE_NOTHROW(tpch::dbgen(sf, db.catalog));
	// con.Query("COPY lineitem to 'lineitem.csv' DELIMITER '|'");
	// return;

	// // test the queries
	tpch::dbgen(sf, db.catalog);
	// result = con.Query("COPY lineitem TO 'lineitem.csv' DELIMITER '|'");
	// CHECK_COLUMN(result, 0, {600572});

	// RESULT_NO_ERROR(result);
	// // check if all the counts are correct
	result = con.Query("SELECT COUNT(*) FROM orders");
	CHECK_COLUMN(result, 0, {150000});
	result = con.Query("SELECT COUNT(*) FROM lineitem");
	CHECK_COLUMN(result, 0, {600572});
	result = con.Query("SELECT COUNT(*) FROM part");
	CHECK_COLUMN(result, 0, {20000});
	result = con.Query("SELECT COUNT(*) FROM partsupp");
	CHECK_COLUMN(result, 0, {80000});
	result = con.Query("SELECT COUNT(*) FROM supplier");
	CHECK_COLUMN(result, 0, {1000});
	result = con.Query("SELECT COUNT(*) FROM customer");
	CHECK_COLUMN(result, 0, {15000});
	result = con.Query("SELECT COUNT(*) FROM nation");
	CHECK_COLUMN(result, 0, {25});
	result = con.Query("SELECT COUNT(*) FROM region");
	CHECK_COLUMN(result, 0, {5});

	// FIXME: checks?
	result = con.Query("SELECT * FROM orders");
	result = con.Query("SELECT * FROM lineitem");
	result = con.Query("SELECT * FROM part");
	result = con.Query("SELECT * FROM partsupp");
	result = con.Query("SELECT * FROM supplier");
	result = con.Query("SELECT * FROM customer");
	result = con.Query("SELECT * FROM nation");
	result = con.Query("SELECT * FROM region");

	result = con.Query("SELECT SUM(l_quantity) FROM lineitem");
	CHECK_COLUMN(result, 0, {15334802});
	result = con.Query("SELECT l_quantity % 5 AS f, COUNT(*) FROM lineitem "
	                   "GROUP BY f ORDER BY f;");
	CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4});
	CHECK_COLUMN(result, 1, {119525, 120331, 120426, 119986, 120304});
	result = con.Query("SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM "
	                   "lineitem GROUP BY l_returnflag;");
	CHECK_COLUMN(result, 0, {"A", "N", "R"});
	CHECK_COLUMN(result, 1, {3774200, 7775079, 3785523});
	CHECK_COLUMN(result, 2, {147790, 304481, 148301});
	result = con.Query(
	    "SELECT l_returnflag, SUM(l_quantity), COUNT(*) FROM lineitem WHERE "
	    "l_shipdate <= cast('1998-09-02' as date) GROUP BY l_returnflag;");
	CHECK_COLUMN(result, 0, {"A", "N", "R"});
	CHECK_COLUMN(result, 1, {3774200, 7554554, 3785523});
	CHECK_COLUMN(result, 2, {147790, 295765, 148301});

	result = con.Query(tpch::get_query(1));
	RESULT_NO_ERROR(result);
	string error_message;
	if (!tpch::check_result(sf, 1, *result.get(), error_message)) {
		FAIL(error_message);
	}
}
