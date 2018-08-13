
#include "catch.hpp"
#include "dbgen.hpp"

#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("[SLOW] Test TPC-H SF0.01", "[tpch]") {
	unique_ptr<DuckDBResult> result;
	double sf = 0.1;

	// generate the TPC-H data for SF 0.1
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	// REQUIRE_NOTHROW(tpch::dbgen(sf, db.catalog));
	// con.Query("COPY lineitem to 'lineitem.csv' DELIMITER '|'");
	// return;

	// // test the queries
	tpch::dbgen(0, db.catalog);
	result = con.Query("COPY lineitem FROM 'lineitem.csv' DELIMITER '|'");
	RESULT_NO_ERROR(result);
	// // check if all the counts are correct
	// result = con.Query("SELECT COUNT(*) FROM orders");
	// REQUIRE(CHECK_COLUMN(result, 0, {135000}));
	result = con.Query("SELECT COUNT(*) FROM lineitem");
	CHECK_COLUMN(result, 0, {600501});
	// result = con.Query("SELECT COUNT(*) FROM part");
	// CHECK_COLUMN(result, 0, {18000});
	// result = con.Query("SELECT COUNT(*) FROM partsupp");
	// CHECK_COLUMN(result, 0, {72000});
	// result = con.Query("SELECT COUNT(*) FROM supplier");
	// CHECK_COLUMN(result, 0, {900});
	// result = con.Query("SELECT COUNT(*) FROM customer");
	// CHECK_COLUMN(result, 0, {13500});
	// result = con.Query("SELECT COUNT(*) FROM nation");
	// CHECK_COLUMN(result, 0, {25});
	// result = con.Query("SELECT COUNT(*) FROM region");
	// CHECK_COLUMN(result, 0, {5});

	// result = con.Query("SELECT * FROM orders");
	// result = con.Query("SELECT * FROM lineitem");
	// result = con.Query("SELECT * FROM part");
	// result = con.Query("SELECT * FROM partsupp");
	// result = con.Query("SELECT * FROM supplier");
	// result = con.Query("SELECT * FROM customer");
	// result = con.Query("SELECT * FROM nation");
	// result = con.Query("SELECT * FROM region");

	result = con.Query(tpch::get_query(1));
	RESULT_NO_ERROR(result);
	string error_message;
	if (!tpch::check_result(sf, 1, *result.get(), error_message)) {
		FAIL(error_message);
	}
}
