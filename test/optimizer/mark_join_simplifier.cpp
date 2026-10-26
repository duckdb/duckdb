#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/mark_join_simplifier.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/logical_plan_verifier.hpp"

using namespace duckdb;

static unique_ptr<Expression> MarkerReference() {
	return make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(TableIndex(3), ProjectionIndex(0)));
}

static unique_ptr<LogicalOperator> FilteredMarkJoin(bool project_marker) {
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
	join->mark_index = TableIndex(3);
	join->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(2), ProjectionIndex(0))),
	    ExpressionType::COMPARE_EQUAL);
	join->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(1)));
	join->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(2)));
	auto filter = make_uniq<LogicalFilter>(MarkerReference());
	filter->children.push_back(std::move(join));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(project_marker ? MarkerReference() : make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	auto projection = make_uniq<LogicalProjection>(TableIndex(4), std::move(expressions));
	projection->children.push_back(std::move(filter));
	return std::move(projection);
}

TEST_CASE("MARK simplification recomputes marker uses and is idempotent", "[optimizer][mark_join_simplifier]") {
	DuckDB db(nullptr);
	Connection con(db);
	MarkJoinSimplifier simplifier(*con.context);
	auto plan = FilteredMarkJoin(true);
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
	auto bindings = plan->GetColumnBindings();
	simplifier.Optimize(plan);
	REQUIRE(plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER);
	REQUIRE(plan->children[0]->children[0]->Cast<LogicalComparisonJoin>().join_type == JoinType::MARK);
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
	REQUIRE(plan->GetColumnBindings() == bindings);

	// Removing the remaining marker use makes this same plan eligible.
	plan->expressions[0] = make_uniq<BoundConstantExpression>(Value::INTEGER(42));
	simplifier.Optimize(plan);
	REQUIRE(plan->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	REQUIRE(plan->children[0]->Cast<LogicalComparisonJoin>().join_type == JoinType::SEMI);
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
	auto first_result = plan->ToString();
	simplifier.Optimize(plan);
	REQUIRE(plan->ToString() == first_result);
	REQUIRE(plan->GetColumnBindings() == bindings);
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
}

TEST_CASE("MARK to ANTI requires a two-valued complete predicate", "[optimizer][mark_join_simplifier]") {
	DuckDB db(nullptr);
	Connection con(db);
	MarkJoinSimplifier simplifier(*con.context);
	auto plan = FilteredMarkJoin(false);
	auto &filter = plan->children[0]->Cast<LogicalFilter>();
	auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();
	join.conditions[0].SetComparisonType(ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	auto not_marker = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	not_marker->GetChildrenMutable().push_back(MarkerReference());
	filter.expressions[0] = std::move(not_marker);

	SECTION("nullable residual") {
		join.conditions.emplace_back(make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN)));
		simplifier.Optimize(plan);
		REQUIRE(join.join_type == JoinType::MARK);
		REQUIRE(filter.expressions.size() == 1);
	}
	SECTION("two-valued residual") {
		join.conditions.emplace_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		simplifier.Optimize(plan);
		REQUIRE(plan->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		REQUIRE(join.join_type == JoinType::ANTI);
		REQUIRE(join.conditions.size() == 2);
	}
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
}

TEST_CASE("MARK simplification preserves root outputs and explicit projection maps",
          "[optimizer][mark_join_simplifier]") {
	DuckDB db(nullptr);
	Connection con(db);
	MarkJoinSimplifier simplifier(*con.context);
	auto plan = FilteredMarkJoin(false);
	SECTION("root marker output") {
		plan = std::move(plan->children[0]);
		auto bindings = plan->GetColumnBindings();
		simplifier.Optimize(plan);
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(plan->GetColumnBindings() == bindings);
	}
	SECTION("explicit filter map") {
		auto &filter = plan->children[0]->Cast<LogicalFilter>();
		filter.projection_map = {ProjectionIndex(0)};
		simplifier.Optimize(plan);
		REQUIRE(plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(filter.children[0]->Cast<LogicalComparisonJoin>().join_type == JoinType::MARK);
	}
	SECTION("ancestor map after the marker column") {
		auto &join = plan->children[0]->children[0]->Cast<LogicalComparisonJoin>();
		auto cross_product =
		    make_uniq<LogicalCrossProduct>(std::move(plan->children[0]), make_uniq<LogicalDummyScan>(TableIndex(5)));
		auto filter = make_uniq<LogicalFilter>(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		filter->projection_map = {ProjectionIndex(2)};
		filter->children.push_back(std::move(cross_product));
		plan->children[0] = std::move(filter);
		simplifier.Optimize(plan);
		REQUIRE(join.join_type == JoinType::MARK);
		REQUIRE(plan->children[0]->GetColumnBindings() ==
		        vector<ColumnBinding> {ColumnBinding(TableIndex(5), ProjectionIndex(0))});
	}
	REQUIRE(LogicalPlanVerifier::VerifyAlways(*plan).IsSuccess());
}
