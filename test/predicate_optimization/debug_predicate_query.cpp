#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Debug Filter Predicate Optimization", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(1, db);
    REQUIRE_NO_FAIL(con.Query("SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0 AND l_comment LIKE '%str%';"));
}