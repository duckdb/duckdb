#include "catch.hpp"
#include "test_helpers.hpp"
#include "expression_helper.hpp"

#include "optimizer/ca_optimizer.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test common aggregate optimizer", "[aggregations]") {
	DuckDB db(nullptr);
	Connection con(db);

    con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE integers(i INTEGER)");

    // now we expect duplicate aggregates to be reduced to a single occurence.
	ExpressionHelper helper(con.context);
    auto tree = helper.ParseLogicalTree("SELECT SUM(i) as a, SUM(i) as b FROM integers");

	REQUIRE(tree->type == LogicalOperatorType::PROJECTION);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY);

	auto aggregate = static_cast<LogicalAggregate*>(tree->children[0].get());

	REQUIRE(tree->expressions[0]->type == ExpressionType::BOUND_COLUMN_REF);
	REQUIRE(tree->expressions[1]->type == ExpressionType::BOUND_COLUMN_REF);

	auto a = static_cast<BoundColumnRefExpression*>(tree->expressions[0].get());
	auto b = static_cast<BoundColumnRefExpression*>(tree->expressions[1].get());

	REQUIRE(a->binding.table_index == aggregate->aggregate_index);
	REQUIRE(b->binding.table_index == aggregate->aggregate_index);

	/* before optimization
	PROJECTION[a, b] // a, b point to different aggregate expressions.
	AGGREGATE_AND_GROUP_BY[a(i), b(i)]
		GET(integers)
	*/
	REQUIRE(aggregate->expressions.size() == 2);
	REQUIRE(a->binding.column_index != b->binding.column_index);

    CommonAggregateOptimizer optimizer;
	optimizer.VisitOperator(*tree);

	/* after optimization
	PROJECTION[a, b] // a, b point to the same single aggregate expression.
	AGGREGATE_AND_GROUP_BY[a(i)]
    GET(integers)
	*/
	REQUIRE(aggregate->expressions.size() == 1);
	REQUIRE(a->binding.column_index == 0);
	REQUIRE(a->binding.column_index == b->binding.column_index);
}
