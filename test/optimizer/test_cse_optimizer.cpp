#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/cse_optimizer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test CSE Optimizer", "[optimizer]") {
	ExpressionHelper helper;
	auto &con = helper.con;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	CommonSubExpressionOptimizer optimizer;

	// simple CSE
	auto tree = helper.ParseLogicalTree("SELECT i+1, i+1 FROM integers");
	optimizer.VisitOperator(*tree);
	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->expressions[0]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(tree->expressions[1]->type == ExpressionType::COMMON_SUBEXPRESSION);

	// more complex CSE
	tree = helper.ParseLogicalTree("SELECT i+(i+1), i+1 FROM integers");
	optimizer.VisitOperator(*tree);
	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->expressions[1]->type == ExpressionType::COMMON_SUBEXPRESSION);

	// more CSEs
	tree = helper.ParseLogicalTree("SELECT i*2, i+1, i*2, i+1, (i+1)+(i*2) FROM integers");
	optimizer.VisitOperator(*tree);
	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->expressions[0]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(tree->expressions[1]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(tree->expressions[2]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(tree->expressions[3]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(tree->expressions[4]->type == ExpressionType::BOUND_FUNCTION);
	auto &op = (BoundFunctionExpression &)*tree->expressions[4];
	REQUIRE(op.children[0]->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(op.children[1]->type == ExpressionType::COMMON_SUBEXPRESSION);

	// test CSEs in WHERE clause
	tree = helper.ParseLogicalTree("SELECT i FROM integers WHERE i+1>10 AND i+1<20");
	optimizer.VisitOperator(*tree);
	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::FILTER);
	auto &filter = *tree->children[0];
	REQUIRE(filter.expressions[0]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
	REQUIRE(filter.expressions[1]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
	auto &lcomp = (BoundComparisonExpression &)*filter.expressions[0];
	auto &rcomp = (BoundComparisonExpression &)*filter.expressions[1];
	REQUIRE(lcomp.left->type == ExpressionType::COMMON_SUBEXPRESSION);
	REQUIRE(rcomp.left->type == ExpressionType::COMMON_SUBEXPRESSION);
}

TEST_CASE("CSE NULL*MIN(42) defense", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	auto result = con.Query("SELECT NULL * MIN(42);");
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}
