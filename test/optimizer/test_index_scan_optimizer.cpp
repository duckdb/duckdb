#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/index_scan.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/common_subexpression.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Index Scan Optimizer for Integers", "[index-optimizer-int]") {
	ExpressionHelper helper;
	auto &con = helper.con;
    string int_types[1] = {"integers"}; // {"tinyint", "smallint", "integer", "bigint"};

    for (int idx = 0; idx < 1; idx ++ ) {
        REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i "+int_types[idx]+")"));
        //! Checking Order Index
        con.Query("CREATE INDEX i_index ON integers using order_index(i)");
        // Checking if Optimizer is using index in simple case
        auto tree = helper.ParseLogicalTree("SELECT i FROM integers where i > CAST(10 AS " + int_types[idx] + ")");
        IndexScan index_scan;
        auto plan = index_scan.Optimize(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::INDEX_SCAN);
        con.Query("DROP INDEX i_index");

        //! Checking ART
        con.Query("CREATE INDEX i_index ON integers using art(i)");
        // Checking if Optimizer is using index in simple case
        tree = helper.ParseLogicalTree("SELECT i FROM integers where i = 10");
        plan = index_scan.Optimize(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::INDEX_SCAN);
    }
}
