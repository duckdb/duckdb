#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/predicate_pushdown.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "test_helpers.hpp"
#include <vector>
using namespace duckdb;
using namespace std;

TEST_CASE("Test Table Filter Push Down", "[filterpushdown-optimizer]") {
    ExpressionHelper helper;
    auto &con = helper.con;

    for (int idx = 0; idx < 1; idx++) {
        REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer )"));
        //! Checking if Optimizer push predicates down
        auto tree = helper.ParseLogicalTree("SELECT i FROM integers where i > 10 ");
        PredicatePushdown predicatePushdown;
        //! The generated plan should be Projection -> Get
        auto plan = predicatePushdown.Optimize(move(tree));
        REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
        REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::INDEX_SCAN);
        con.Query("DROP INDEX i_index");
    }
}
