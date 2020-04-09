#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "test_helpers.hpp"
#include <duckdb/optimizer/like_optimizer.hpp>
#include <duckdb/optimizer/optimizer.hpp>

using namespace duckdb;
using namespace std;

TEST_CASE("Test Like Optimizer", "[like-optimizer]") {
    ExpressionHelper helper;
    auto &con = helper.con;
    Binder binder(*con.context);
    Optimizer optimizer(binder, *con.context);
    con.Query("CREATE TABLE strings(s VARCHAR)");
    con.Query("INSERT INTO strings VALUES ('hello', 'world', 'pattern')");

    SECTION("Like Optimizer Prefix") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE 'h%'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "h%");

		LikeOptimizer like_opt(optimizer);
		auto pref_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(pref_plan->type == LogicalOperatorType::FILTER);

		auto &pref_func_expr = (BoundFunctionExpression &) *(pref_plan->expressions[0]).get();
		assert(pref_func_expr.function.name == "prefix");

	    pattern = ExpressionExecutor::EvaluateScalar(*pref_func_expr.children[1]);
	    assert(pattern.str_value == "h");
    }

    SECTION("Like Optimizer Suffix") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE '%h'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "%h");

		LikeOptimizer like_opt(optimizer);
		auto suff_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(suff_plan->type == LogicalOperatorType::FILTER);

		auto &suff_func_expr = (BoundFunctionExpression &) *(suff_plan->expressions[0]).get();
		assert(suff_func_expr.function.name == "suffix");

	    pattern = ExpressionExecutor::EvaluateScalar(*suff_func_expr.children[1]);
	    assert(pattern.str_value == "h");
    }

    SECTION("Like Optimizer Contains") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE '%h%'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "%h%");

		LikeOptimizer like_opt(optimizer);
		auto contains_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(contains_plan->type == LogicalOperatorType::FILTER);

		auto &contains_func_expr = (BoundFunctionExpression &) *(contains_plan->expressions[0]).get();
		assert(contains_func_expr.function.name == "contains");

	    pattern = ExpressionExecutor::EvaluateScalar(*contains_func_expr.children[1]);
	    assert(pattern.str_value == "h");
    }
}
