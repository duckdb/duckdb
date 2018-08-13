
#include "catch.hpp"
#include "dbgen.hpp"

#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("[SLOW] Test TPC-H SF0.01", "[tpch]") {
	float sf = 0.01;

	// generate the TPC-H data for SF 0.01
	DuckDB db(nullptr);
	REQUIRE_NOTHROW(tpch::dbgen(sf, db.catalog));

	// test the queries
	DuckDBConnection con(db);

	// check if all the counts are correct
	unique_ptr<DuckDBResult> result;
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM orders"));
	REQUIRE(CHECK_COLUMN(result, 0, {13500}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM lineitem"));
	REQUIRE(CHECK_COLUMN(result, 0, {54178}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM part"));
	REQUIRE(CHECK_COLUMN(result, 0, {1800}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM partsupp"));
	REQUIRE(CHECK_COLUMN(result, 0, {7200}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM supplier"));
	REQUIRE(CHECK_COLUMN(result, 0, {90}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM customer"));
	REQUIRE(CHECK_COLUMN(result, 0, {1350}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM nation"));
	REQUIRE(CHECK_COLUMN(result, 0, {25}));
	REQUIRE_NOTHROW(result = con.Query("SELECT COUNT(*) FROM region"));
	REQUIRE(CHECK_COLUMN(result, 0, {5}));

	// FIXME
	REQUIRE_NOTHROW(result = con.Query("SELECT * FROM orders"));

	//REQUIRE_NOTHROW(result = con.Query(tpch::get_query(1)));
	//REQUIRE(tpch::check_result(sf, 1, *result.get()));
}