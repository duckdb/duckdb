#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"

#include <chrono>
#include <iostream>
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-H SF0.01 using streaming api", "[tpch][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	double sf = 0.01;
	if (!db.ExtensionIsLoaded("tpch")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=" + to_string(sf) + ")"));

	for (idx_t tpch_num = 1; tpch_num <= 22; tpch_num++) {
		result = con.SendQuery("pragma tpch(" + to_string(tpch_num) + ");");

		duckdb::ColumnDataCollection collection(duckdb::Allocator::DefaultAllocator(), result->types);

		while (true) {
			auto chunk = result->Fetch();
			if (chunk) {
				collection.Append(*chunk);
			} else {
				break;
			}
		}

		COMPARE_CSV_COLLECTION(collection, TpchExtension::GetAnswer(sf, tpch_num), true);
	}
}
