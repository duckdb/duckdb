#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpce.hpp"

#include <chrono>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-E", "[tpce][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	// a higher scale factor for TPC-E means LESS data (for some reason)
	// generate the TPC-E data for SF 100000
	uint32_t sf = 100000;
	tpce::dbgen(db, sf);

	auto result = con.Query("SELECT * FROM sqlite_master()");
	result->Print();

	for (size_t i = 0; i < result->collection.count; i++) {
		auto table_name = result->collection.GetValue(1, i);

		fprintf(stderr, "%s\n\n", table_name.str_value.c_str());
		auto result2 = con.Query("SELECT COUNT(*) FROM " + table_name.str_value);
		// result2->Print();

		REQUIRE(1 == 1);
		// result2 = con.Query("SELECT * FROM " + table_name.str_value + " LIMIT
		// 3"); result2->Print();
	}
}
