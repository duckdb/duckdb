#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Debug Filter Predicate 1", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(0.1, db);
    REQUIRE_NO_FAIL(con.Query("SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 50 == 0 AND l_comment LIKE '%str%';"));
}

TEST_CASE("Debug Filter Predicate 2", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(0.1, db);
    REQUIRE_NO_FAIL(con.Query("SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%str%' AND l_orderkey % 50 == 0;"));
}

TEST_CASE("Debug Filter Predicate 3", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(0.1, db);
    REQUIRE_NO_FAIL(con.Query("SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 50 == 0;"));
}

TEST_CASE("Debug Filter Predicate 4", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	tpch::dbgen(0.1, db);
    REQUIRE_NO_FAIL(con.Query("SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%str%';"));
}