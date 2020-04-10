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
	unique_ptr<QueryResult> result;
    ExpressionHelper helper;
    auto &con = helper.con;
    Binder binder(*con.context);
    Optimizer optimizer(binder, *con.context);
    con.Query("CREATE TABLE strings(s VARCHAR)");
    con.Query("INSERT INTO strings VALUES ('hello'), ('world'), ('pattern'), (NULL)");

    SECTION("Like Optimizer Prefix") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE 'w%'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		// testing if the function is a scalar LIKE
		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

		// testing if the string pattern is the same as in the query
	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "w%");

		LikeOptimizer like_opt(optimizer);
		auto pref_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(pref_plan->type == LogicalOperatorType::FILTER);

		// testing if the optimizer transform LIKE into a scalar prefix function
		auto &pref_func_expr = (BoundFunctionExpression &) *(pref_plan->expressions[0]).get();
		assert(pref_func_expr.function.name == "prefix");

		// testing if the string pattern was transformed to work in the prefix scalar function
	    pattern = ExpressionExecutor::EvaluateScalar(*pref_func_expr.children[1]);
	    assert(pattern.str_value == "w");

	    result = con.Query("SELECT s FROM strings WHERE s LIKE 'w%'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"world"}));

	    // testing multiples '%'
	    result = con.Query("SELECT s FROM strings WHERE s LIKE 'w%%%%%'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"world"}));
    }

    SECTION("Like Optimizer Suffix") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE '%tern'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		// testing if the function is a scalar LIKE
		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

		// testing if the string pattern is the same as in the query
	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "%tern");

		LikeOptimizer like_opt(optimizer);
		auto suff_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(suff_plan->type == LogicalOperatorType::FILTER);

		// testing if the optimizer transform LIKE into a scalar suffix function
		auto &suff_func_expr = (BoundFunctionExpression &) *(suff_plan->expressions[0]).get();
		assert(suff_func_expr.function.name == "suffix");

		// testing if the string pattern was transformed to work in the suffix scalar function
	    pattern = ExpressionExecutor::EvaluateScalar(*suff_func_expr.children[1]);
	    assert(pattern.str_value == "tern");

	    result = con.Query("SELECT s FROM strings WHERE s LIKE '%tern'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"pattern"}));

	    // testing multiples '%'
	    result = con.Query("SELECT s FROM strings WHERE s LIKE '%%%%%%%%tern'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"pattern"}));
    }

    SECTION("Like Optimizer Contains") {
		auto tree = helper.ParseLogicalTree("SELECT s FROM strings WHERE s LIKE '%h%'");
		assert(tree->children[0]->type == LogicalOperatorType::FILTER);

		// testing if the function is a scalar LIKE
		auto &like_func_expr = (BoundFunctionExpression &) *(tree->children[0]->expressions[0]).get();
		assert(like_func_expr.function.name == "~~");

		// testing if the string pattern is the same as in the query
	    Value pattern = ExpressionExecutor::EvaluateScalar(*like_func_expr.children[1]);
	    assert(pattern.str_value == "%h%");

		LikeOptimizer like_opt(optimizer);
		auto contains_plan = like_opt.Rewrite(move(tree->children[0]));
		assert(contains_plan->type == LogicalOperatorType::FILTER);

		// testing if the optimizer transform LIKE into a scalar contains function
		auto &contains_func_expr = (BoundFunctionExpression &) *(contains_plan->expressions[0]).get();
		assert(contains_func_expr.function.name == "contains");

		// testing if the string pattern was transformed to work in the contains scalar function
	    pattern = ExpressionExecutor::EvaluateScalar(*contains_func_expr.children[1]);
	    assert(pattern.str_value == "h");

	    result = con.Query("SELECT s FROM strings WHERE s LIKE '%h%'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));

	    // testing multiples '%'
	    result = con.Query("SELECT s FROM strings WHERE s LIKE '%%h%%%%'");
	    REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
    }
}
