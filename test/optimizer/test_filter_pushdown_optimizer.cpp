#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "expression_helper.hpp"
#include "test_helpers.hpp"

#include <duckdb/optimizer/filter_pushdown.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <vector>
using namespace duckdb;
using namespace std;

TEST_CASE("Test Table Filter Push Down", "[filterpushdown-optimizer]") {
    ExpressionHelper helper;
    auto &con = helper.con;
    Binder binder(*con.context);
    Optimizer opt(binder,*con.context);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer, k integer )"));
    //! Checking if Optimizer push predicates down
    auto tree = helper.ParseLogicalTree("SELECT k FROM integers where i+j > 10 and j = 5 and i = k+1 ");
    FilterPushdown predicatePushdown(opt);
    //! The generated plan should be Projection -> Filter (2) ->Get (1)
    auto plan = predicatePushdown.Rewrite(move(tree));
    REQUIRE(plan->children[0]->type == LogicalOperatorType::FILTER);
    REQUIRE(plan->children[0]->expressions.size() == 2);

    REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
    REQUIRE(plan->children[0]->children[0]->expressions.size()==1);

    tree = helper.ParseLogicalTree("SELECT k FROM integers where  j = 5 ");
    //! The generated plan should be Projection -> Get (1)
    plan = predicatePushdown.Rewrite(move(tree));
    REQUIRE(plan->children[0]->type == LogicalOperatorType::GET);
    REQUIRE(plan->children[0]->expressions.size() == 1);
}

TEST_CASE("Test Table Filter Push Down Scan", "[filterpushdown-optimizer]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);
    Connection con(db);

    vector<int> input;
    idx_t input_size = 1000;
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer)"));
    for (idx_t i = 0; i < input_size; ++i){
        input.push_back(i);
    }
//    random_shuffle(input.begin(),input.end());
    for (idx_t i = 0; i < input_size; ++i){
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES("+ to_string(input[i])+ "," + to_string(input[i]) +  ")"));
    }

    result = con.Query("SELECT i FROM integers where j = 99000 ");
    REQUIRE(CHECK_COLUMN(result, 0, {99000}));

}