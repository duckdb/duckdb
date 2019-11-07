#include "catch.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/ca_optimizer.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test common aggregate optimizer", "[aggregations]") {
	LogicalProjection *projection;
	LogicalFilter *filter;
	LogicalWindow *window;
	LogicalAggregate *aggregate;

	// now we expect duplicate aggregates to be reduced to a single occurence.
	ExpressionHelper helper;
	CommonAggregateOptimizer optimizer;

	helper.con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");

	/* before optimization
	PROJECTION[a, b] // a, b point to different aggregate expressions.
	AGGREGATE_AND_GROUP_BY[a(i), b(i)]
	    GET(integers)
	*/
	auto tree = helper.ParseLogicalTree("SELECT SUM(i) as a, SUM(i) as b FROM integers");

	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY);

	aggregate = static_cast<LogicalAggregate *>(tree->children[0].get());

	REQUIRE(tree->expressions[0]->type == ExpressionType::BOUND_COLUMN_REF);
	REQUIRE(tree->expressions[1]->type == ExpressionType::BOUND_COLUMN_REF);

	auto a = static_cast<BoundColumnRefExpression *>(tree->expressions[0].get());
	auto b = static_cast<BoundColumnRefExpression *>(tree->expressions[1].get());

	REQUIRE(a->binding.table_index == aggregate->aggregate_index);
	REQUIRE(b->binding.table_index == aggregate->aggregate_index);
	REQUIRE(aggregate->expressions.size() == 2);
	REQUIRE(a->binding.column_index != b->binding.column_index);
	optimizer.VisitOperator(*tree);

	/* after optimization
	PROJECTION[a, b] // a, b point to the same single aggregate expression.
	AGGREGATE_AND_GROUP_BY[a(i)]
	GET(integers)
	*/

	// There is only one aggregate expression.
	REQUIRE(aggregate->expressions.size() == 1);

	// bound column referencesses a and b point to the same aggregate expression.
	REQUIRE(a->binding.column_index == 0);
	REQUIRE(a->binding.column_index == b->binding.column_index);

	/*
	PROJECTION[SUM + 2 * SUM]
	FILTER[SUM>0]
	    AGGREGATE_AND_GROUP_BY[SUM(i), SUM(i), SUM(i)][j]
	        GET(integers)
	*/
	tree = helper.ParseLogicalTree("SELECT (SUM(i) + SUM(i)) as b FROM integers GROUP BY j HAVING SUM(i) > 0");

	filter = static_cast<LogicalFilter *>(tree->children[0].get());

	aggregate = static_cast<LogicalAggregate *>(filter->children[0].get());

	REQUIRE(aggregate->expressions.size() == 3);

	optimizer.VisitOperator(*tree);

	/* after optimization
	PROJECTION[SUM + SUM]
	FILTER[SUM>0]
	    AGGREGATE_AND_GROUP_BY[SUM(i)][j]
	        GET(integers)
	*/
	REQUIRE(aggregate->expressions.size() == 1);

	auto &op_expression = (BoundFunctionExpression &)*tree->expressions[0];
	// Left SUM expression of addition operator expression.
	auto &sum_expression_l = (BoundColumnRefExpression &)*op_expression.children[0];
	// Right SUM expression of addition operator expression.
	auto &sum_expression_r = (BoundColumnRefExpression &)*op_expression.children[1];
	// Left SUM side of comparison expression.
	auto &comp_expression = (BoundComparisonExpression &)*filter->expressions[0];
	auto &filter_expression = (BoundColumnRefExpression &)*comp_expression.left;

	// sum_expressions all point to the same column index
	REQUIRE(sum_expression_l.binding.column_index == 0);
	REQUIRE(sum_expression_r.binding.column_index == 0);
	REQUIRE(filter_expression.binding.column_index == 0);

	/*
	ORDER_BY
	PROJECTION[i, SUM, w]
	    WINDOW[WINDOW]
	        AGGREGATE_AND_GROUP_BY[SUM(j), SUM(j), SUM(j), SUM(j), SUM(j)][i]
	            GET(integers)
	*/
	tree = helper.ParseLogicalTree("SELECT i, SUM(j), AVG(SUM(j)+SUM(j))"
	                               " OVER (PARTITION BY SUM(j) ORDER BY SUM(j)) as w"
	                               " FROM integers GROUP BY i ORDER BY i;");

	projection = static_cast<LogicalProjection *>(tree->children[0].get());
	window = static_cast<LogicalWindow *>(projection->children[0].get());
	aggregate = static_cast<LogicalAggregate *>(window->children[0].get());

	REQUIRE(aggregate->expressions.size() == 5);

	optimizer.VisitOperator(*tree);

	/* after optimization
	ORDER_BY
	PROJECTION[i, SUM, w]
	    WINDOW[WINDOW]
	        AGGREGATE_AND_GROUP_BY[SUM(j)][i]
	            GET(integers)
	*/
	REQUIRE(aggregate->expressions.size() == 1);

	// sum expression corresponding to the partition in the over clause.
	auto &window_expression = (BoundWindowExpression &)*window->expressions[0];
	REQUIRE(window_expression.children.size() == 1);
	REQUIRE(window_expression.children[0]->type == ExpressionType::OPERATOR_CAST);
	auto &cast_op = (BoundCastExpression &)*window_expression.children[0];
	REQUIRE(cast_op.child->type == ExpressionType::BOUND_FUNCTION);
	auto &bound_op = (BoundFunctionExpression &)*cast_op.child;
	REQUIRE(bound_op.children.size() == 2);
	auto &sum_expression_left = (BoundColumnRefExpression &)*bound_op.children[0];
	auto &sum_expression_right = (BoundColumnRefExpression &)*bound_op.children[1];
	REQUIRE(sum_expression_left.binding.column_index == 0);
	REQUIRE(sum_expression_right.binding.column_index == 0);
}
