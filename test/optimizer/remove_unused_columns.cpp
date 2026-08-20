#include "catch.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

static unique_ptr<LogicalProjection> CreatePrunableProjection(TableIndex table_index) {
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	auto projection = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	projection->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(table_index.index + 1000)));
	return projection;
}

static void ResolveTypes(LogicalOperator &op) {
	for (auto &child : op.children) {
		ResolveTypes(*child);
	}
	op.ResolveOperatorTypes();
}

static void ApplyColumnPruning(Connection &connection, unique_ptr<LogicalOperator> &plan) {
	connection.BeginTransaction();
	ResolveTypes(*plan);
	{
		auto binder = Binder::CreateBinder(*connection.context);
		Optimizer optimizer(*binder, *connection.context);
		RemoveUnusedColumns remove_unused(optimizer);
		remove_unused.VisitOperator(plan);
	}
	connection.Rollback();
}

TEST_CASE("Column pruning removes dead projection map entries", "[optimizer][unused_columns]") {
	DuckDB db;
	Connection connection(db);
	const auto child_table = TableIndex(1000);
	const auto output_table = TableIndex(1001);

	auto child = CreatePrunableProjection(child_table);
	auto filter = make_uniq<LogicalFilter>(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(1))));
	filter->projection_map = {ProjectionIndex(0), ProjectionIndex(2)};
	filter->children.push_back(std::move(child));
	auto &filter_ref = *filter;

	vector<unique_ptr<Expression>> output_expressions;
	output_expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(0))));
	auto output = make_uniq<LogicalProjection>(output_table, std::move(output_expressions));
	output->children.push_back(std::move(filter));
	unique_ptr<LogicalOperator> plan = std::move(output);

	ApplyColumnPruning(connection, plan);

	REQUIRE(filter_ref.projection_map == vector<ProjectionIndex> {ProjectionIndex(0)});
	REQUIRE(filter_ref.children[0]->GetColumnBindings() ==
	        vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0)),
	                               ColumnBinding(child_table, ProjectionIndex(1))});
	REQUIRE(filter_ref.GetColumnBindings() == vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0))});
}

TEST_CASE("Column pruning remaps retained projection map entries", "[optimizer][unused_columns]") {
	DuckDB db;
	Connection connection(db);
	const auto child_table = TableIndex(1000);
	const auto output_table = TableIndex(1001);

	auto child = CreatePrunableProjection(child_table);
	auto filter = make_uniq<LogicalFilter>(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(1))));
	filter->projection_map = {ProjectionIndex(2)};
	filter->children.push_back(std::move(child));
	auto &filter_ref = *filter;

	vector<unique_ptr<Expression>> output_expressions;
	output_expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(2))));
	auto output = make_uniq<LogicalProjection>(output_table, std::move(output_expressions));
	output->children.push_back(std::move(filter));
	unique_ptr<LogicalOperator> plan = std::move(output);

	ApplyColumnPruning(connection, plan);

	REQUIRE(filter_ref.projection_map == vector<ProjectionIndex> {ProjectionIndex(1)});
	REQUIRE(filter_ref.children[0]->GetColumnBindings() ==
	        vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0)),
	                               ColumnBinding(child_table, ProjectionIndex(1))});
	REQUIRE(filter_ref.GetColumnBindings() == vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(1))});
}

TEST_CASE("Column pruning follows replacements before matching projection map bindings",
          "[optimizer][unused_columns]") {
	DuckDB db;
	Connection connection(db);
	const auto child_table = TableIndex(1000);
	const auto output_table = TableIndex(1001);

	auto child = CreatePrunableProjection(child_table);
	auto filter = make_uniq<LogicalFilter>(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(2))));
	filter->projection_map = {ProjectionIndex(1)};
	filter->children.push_back(std::move(child));
	auto &filter_ref = *filter;

	vector<unique_ptr<Expression>> output_expressions;
	output_expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(1))));
	auto output = make_uniq<LogicalProjection>(output_table, std::move(output_expressions));
	output->children.push_back(std::move(filter));
	unique_ptr<LogicalOperator> plan = std::move(output);

	ApplyColumnPruning(connection, plan);

	REQUIRE(filter_ref.projection_map == vector<ProjectionIndex> {ProjectionIndex(0)});
	REQUIRE(filter_ref.children[0]->GetColumnBindings() ==
	        vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0)),
	                               ColumnBinding(child_table, ProjectionIndex(1))});
	REQUIRE(filter_ref.GetColumnBindings() == vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0))});
}

TEST_CASE("Column pruning retains a minimum layout for removed projection map entries", "[optimizer][unused_columns]") {
	DuckDB db;
	Connection connection(db);
	const auto child_table = TableIndex(1000);
	const auto output_table = TableIndex(1001);

	auto child = CreatePrunableProjection(child_table);
	auto filter = make_uniq<LogicalFilter>(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(0))));
	filter->expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, ColumnBinding(child_table, ProjectionIndex(1))));
	filter->projection_map = {ProjectionIndex(2)};
	filter->children.push_back(std::move(child));
	auto &filter_ref = *filter;

	vector<unique_ptr<Expression>> output_expressions;
	output_expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	auto output = make_uniq<LogicalProjection>(output_table, std::move(output_expressions));
	output->children.push_back(std::move(filter));
	unique_ptr<LogicalOperator> plan = std::move(output);

	ApplyColumnPruning(connection, plan);

	REQUIRE(filter_ref.projection_map == vector<ProjectionIndex> {ProjectionIndex(0)});
	REQUIRE(filter_ref.children[0]->GetColumnBindings() ==
	        vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0)),
	                               ColumnBinding(child_table, ProjectionIndex(1))});
	REQUIRE(filter_ref.GetColumnBindings() == vector<ColumnBinding> {ColumnBinding(child_table, ProjectionIndex(0))});
}
