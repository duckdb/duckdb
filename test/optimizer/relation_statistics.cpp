#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

using namespace duckdb;

static RelationStats CreateStats(const vector<ColumnBinding> &bindings, const vector<idx_t> &distinct_counts,
                                 idx_t cardinality) {
	REQUIRE(bindings.size() == distinct_counts.size());
	RelationStats result;
	result.cardinality = cardinality;
	result.stats_initialized = true;
	for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
		result.columns.emplace_back(bindings[column_idx],
		                            DistinctCount(distinct_counts[column_idx], DistinctCountSource::EXACT),
		                            Identifier("column_" + to_string(column_idx)));
	}
	result.Verify(bindings);
	return result;
}

static optional_ptr<LogicalOperator> FindOperator(LogicalOperator &op, LogicalOperatorType type) {
	if (op.type == type) {
		return op;
	}
	for (auto &child : op.children) {
		auto result = FindOperator(*child, type);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

class UnsupportedOutputOperator : public LogicalOperator {
public:
	explicit UnsupportedOutputOperator(TableIndex output_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT), output_index(output_index) {
	}

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(output_index, ProjectionIndex(0))};
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::INTEGER};
	}

private:
	TableIndex output_index;
};

TEST_CASE("Relation statistics follow projection output bindings", "[optimizer][relation_statistics]") {
	auto child_table = TableIndex(10);
	auto child_bindings = LogicalOperator::GenerateColumnBindings(child_table, 2);
	auto child_stats = CreateStats(child_bindings, {7, 13}, 20);

	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, child_bindings[1]));
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, child_bindings[0]));
	LogicalProjection projection(TableIndex(20), std::move(expressions));

	auto stats = RelationStatisticsHelper::ExtractProjectionStats(projection, child_stats);
	REQUIRE(stats);
	auto output_bindings = projection.GetColumnBindings();
	REQUIRE(stats->MatchesBindings(output_bindings));
	REQUIRE(stats->columns[0].distinct_count.distinct_count == 13);
	REQUIRE(stats->columns[1].distinct_count.distinct_count == 1);
	REQUIRE(stats->columns[1].distinct_count.source == DistinctCountSource::EXACT);
	REQUIRE(stats->columns[2].distinct_count.distinct_count == 7);
}

TEST_CASE("Aggregate group and result statistics use distinct bindings", "[optimizer][relation_statistics]") {
	auto child_table = TableIndex(10);
	auto child_bindings = LogicalOperator::GenerateColumnBindings(child_table, 2);
	auto child_stats = CreateStats(child_bindings, {5, 17}, 100);

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
	LogicalAggregate aggregate(TableIndex(20), TableIndex(21), std::move(aggregates));
	aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, child_bindings[1]));
	aggregate.grouping_sets.emplace_back();
	aggregate.grouping_sets.back().insert(ProjectionIndex(0));

	auto stats = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats);
	REQUIRE(stats);
	auto output_bindings = aggregate.GetColumnBindings();
	REQUIRE(output_bindings.size() == 2);
	REQUIRE(stats->MatchesBindings(output_bindings));
	REQUIRE(stats->columns.size() == 2);
	REQUIRE(stats->columns[0].binding == ColumnBinding(TableIndex(20), ProjectionIndex(0)));
	REQUIRE(stats->columns[0].distinct_count.distinct_count == 17);
	REQUIRE(stats->columns[1].binding == ColumnBinding(TableIndex(21), ProjectionIndex(0)));

	DuckDB db;
	Connection connection(db);
	RelationManager relation_manager(*connection.context);
	REQUIRE(relation_manager.AddRelation(aggregate, nullptr, *stats));
	ColumnBinding normalized_group;
	ColumnBinding normalized_result;
	REQUIRE(relation_manager.TryNormalizeBinding(output_bindings[0], normalized_group));
	REQUIRE(relation_manager.TryNormalizeBinding(output_bindings[1], normalized_result));
	REQUIRE(normalized_group == ColumnBinding(TableIndex(0), ProjectionIndex(0)));
	REQUIRE(normalized_result == ColumnBinding(TableIndex(0), ProjectionIndex(1)));
}

static unique_ptr<LogicalOperator> CreateProjectedDummy(TableIndex input_index, TableIndex projection_index) {
	auto dummy = make_uniq<LogicalDummyScan>(input_index);
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(input_index, ProjectionIndex(0))));
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(expressions));
	projection->children.push_back(std::move(dummy));
	return std::move(projection);
}

static unique_ptr<LogicalOperator> CreateConstantProjection(TableIndex table_index, idx_t column_count) {
	vector<unique_ptr<Expression>> expressions;
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(NumericCast<int32_t>(column_idx))));
	}
	auto projection = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	projection->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(table_index.index + 100)));
	return std::move(projection);
}

TEST_CASE("Relation statistics extraction is cached by operator", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto plan = CreateProjectedDummy(TableIndex(10), TableIndex(20));
	RelationStatsExtractor extractor(*connection.context);

	auto first = extractor.Extract(*plan);
	REQUIRE(first);
	REQUIRE(first->MatchesBindings(plan->GetColumnBindings()));
	REQUIRE(extractor.ExtractedOperatorCount() == 2);
	auto second = extractor.Extract(*plan);
	REQUIRE(second == first);
	REQUIRE(extractor.ExtractedOperatorCount() == 2);
}

TEST_CASE("Relation statistics follow SQL scan, filter and projection bindings", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE stats_scan AS SELECT i % 10 AS k, i AS v FROM range(100) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("ANALYZE stats_scan"));
	connection.BeginTransaction();
	auto plan = connection.ExtractPlan("SELECT v, k FROM stats_scan WHERE k = 3");
	RelationStatsExtractor extractor(*connection.context);

	auto stats = extractor.Extract(*plan);
	REQUIRE(stats);
	REQUIRE(stats->MatchesBindings(plan->GetColumnBindings()));
	REQUIRE(stats->cardinality > 0);
	REQUIRE(stats->cardinality < 100);
	REQUIRE(stats->columns.size() == 2);
	REQUIRE(stats->columns[0].name == Identifier("v"));
	REQUIRE(stats->columns[1].name == Identifier("k"));
	auto scan_plan = connection.ExtractPlan("SELECT k, v FROM stats_scan");
	RelationStatsExtractor scan_extractor(*connection.context);
	auto scan = FindOperator(*scan_plan, LogicalOperatorType::LOGICAL_GET);
	REQUIRE(scan);
	auto scan_stats = scan_extractor.Extract(*scan);
	REQUIRE(scan_stats);
	REQUIRE(scan_stats->cardinality == 100);
	REQUIRE(scan_stats->columns.size() == 2);
	REQUIRE(scan_stats->columns[0].distinct_count.distinct_count > 0);
	REQUIRE(scan_stats->columns[0].distinct_count.distinct_count <= 100);
	connection.Rollback();
}

TEST_CASE("Operator statistics remain aligned across unary and join operators", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto left_bindings = LogicalOperator::GenerateColumnBindings(TableIndex(10), 2);
	auto right_bindings = LogicalOperator::GenerateColumnBindings(TableIndex(20), 1);
	auto left_stats = CreateStats(left_bindings, {7, 11}, 100);
	auto right_stats = CreateStats(right_bindings, {3}, 4);

	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
	projection_expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(2)));
	auto filter_child = make_uniq<LogicalProjection>(TableIndex(10), std::move(projection_expressions));
	LogicalFilter filter;
	filter.projection_map.push_back(ProjectionIndex(1));
	filter.children.push_back(std::move(filter_child));
	vector<reference<const RelationStats>> unary_children {left_stats};
	auto filter_stats = RelationStatisticsHelper::ExtractOperatorStats(filter, *connection.context, unary_children);
	REQUIRE(filter_stats);
	REQUIRE(filter_stats->cardinality == 20);
	REQUIRE(filter_stats->MatchesBindings(filter.GetColumnBindings()));
	REQUIRE(filter_stats->columns[0].distinct_count.distinct_count == 11);

	auto window_child = CreateProjectedDummy(TableIndex(30), TableIndex(10));
	LogicalWindow window(TableIndex(40));
	window.expressions.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
	window.children.push_back(std::move(window_child));
	auto single_child_stats = CreateStats({ColumnBinding(TableIndex(10), ProjectionIndex(0))}, {7}, 100);
	vector<reference<const RelationStats>> window_children {single_child_stats};
	auto window_stats = RelationStatisticsHelper::ExtractOperatorStats(window, *connection.context, window_children);
	REQUIRE(window_stats);
	REQUIRE(window_stats->MatchesBindings(window.GetColumnBindings()));
	REQUIRE(window_stats->columns.size() == 2);

	auto cross_product = make_uniq<LogicalCrossProduct>(CreateConstantProjection(TableIndex(10), 2),
	                                                    CreateConstantProjection(TableIndex(20), 1));
	vector<reference<const RelationStats>> join_children {left_stats, right_stats};
	auto cross_stats =
	    RelationStatisticsHelper::ExtractOperatorStats(*cross_product, *connection.context, join_children);
	REQUIRE(cross_stats);
	REQUIRE(cross_stats->cardinality == 400);
	REQUIRE(cross_stats->MatchesBindings(cross_product->GetColumnBindings()));

	LogicalComparisonJoin semi_join(JoinType::SEMI);
	semi_join.children.push_back(CreateConstantProjection(TableIndex(10), 2));
	semi_join.children.push_back(CreateConstantProjection(TableIndex(20), 1));
	auto semi_stats = RelationStatisticsHelper::ExtractOperatorStats(semi_join, *connection.context, join_children);
	REQUIRE(semi_stats);
	REQUIRE(semi_stats->cardinality == left_stats.cardinality);
	REQUIRE(semi_stats->MatchesBindings(semi_join.GetColumnBindings()));

	for (auto join_type :
	     {JoinType::INNER, JoinType::LEFT, JoinType::RIGHT, JoinType::OUTER, JoinType::ANTI, JoinType::RIGHT_ANTI}) {
		LogicalComparisonJoin join(join_type);
		join.children.push_back(CreateConstantProjection(TableIndex(10), 2));
		join.children.push_back(CreateConstantProjection(TableIndex(20), 1));
		auto join_stats = RelationStatisticsHelper::ExtractOperatorStats(join, *connection.context, join_children);
		REQUIRE(join_stats);
		REQUIRE(join_stats->MatchesBindings(join.GetColumnBindings()));
		REQUIRE(join_stats->cardinality ==
		        (join_type == JoinType::RIGHT_ANTI ? right_stats.cardinality : left_stats.cardinality));
	}
}

TEST_CASE("Operator statistics reject incomplete child layouts", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto projection = CreateProjectedDummy(TableIndex(10), TableIndex(20));
	auto incomplete_stats = CreateStats({ColumnBinding(TableIndex(11), ProjectionIndex(0))}, {1}, 1);
	vector<reference<const RelationStats>> child_stats {incomplete_stats};
	REQUIRE_FALSE(RelationStatisticsHelper::ExtractOperatorStats(*projection, *connection.context, child_stats));
}

TEST_CASE("Unsupported output operators do not receive guessed statistics", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto child = make_uniq<LogicalDummyScan>(TableIndex(10));
	auto child_stats = CreateStats(child->GetColumnBindings(), {1}, 1);
	UnsupportedOutputOperator unsupported(TableIndex(20));
	unsupported.children.push_back(std::move(child));

	RelationStatsExtractor extractor(*connection.context);
	REQUIRE_FALSE(extractor.Extract(unsupported));

	RelationManager relation_manager(*connection.context);
	REQUIRE_FALSE(relation_manager.AddRelation(unsupported, nullptr, child_stats));
	REQUIRE_FALSE(relation_manager.HasCompleteStats());

	auto same_binding_child = make_uniq<LogicalDummyScan>(TableIndex(30));
	auto same_binding_stats = CreateStats(same_binding_child->GetColumnBindings(), {1}, 1);
	UnsupportedOutputOperator same_binding_unsupported(TableIndex(30));
	same_binding_unsupported.children.push_back(std::move(same_binding_child));
	RelationManager same_binding_manager(*connection.context);
	REQUIRE_FALSE(same_binding_manager.AddRelation(same_binding_unsupported, nullptr, same_binding_stats));
	REQUIRE_FALSE(same_binding_manager.HasCompleteStats());
}

TEST_CASE("Distinct statistics derive cardinality before relation registration", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto child = CreateConstantProjection(TableIndex(30), 2);
	auto child_stats = CreateStats(child->GetColumnBindings(), {5, 7}, 20);
	LogicalDistinct distinct(DistinctType::DISTINCT);
	distinct.children.push_back(std::move(child));

	vector<reference<const RelationStats>> children {child_stats};
	auto stats = RelationStatisticsHelper::ExtractOperatorStats(distinct, *connection.context, children);
	REQUIRE(stats);
	REQUIRE(stats->MatchesBindings(distinct.GetColumnBindings()));
	REQUIRE(stats->cardinality < child_stats.cardinality);
	REQUIRE(RelationStatisticsHelper::EstimateDistinctCardinality({DistinctCount(1, DistinctCountSource::EXACT)}, 1) ==
	        1);

	RelationManager relation_manager(*connection.context);
	REQUIRE(relation_manager.AddRelation(distinct, nullptr, child_stats));
	REQUIRE(relation_manager.HasCompleteStats());
}

TEST_CASE("Generated scans and explain outputs have explicit statistics", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto in_plan = connection.ExtractPlan("SELECT * FROM range(10) t(i) WHERE i IN (1, 2, 3, 4, 5, 6)");
	auto chunk_get = FindOperator(*in_plan, LogicalOperatorType::LOGICAL_CHUNK_GET);
	REQUIRE(chunk_get);
	RelationStatsExtractor chunk_extractor(*connection.context);
	auto chunk_stats = chunk_extractor.Extract(*chunk_get);
	REQUIRE(chunk_stats);
	REQUIRE(chunk_stats->MatchesBindings(chunk_get->GetColumnBindings()));
	REQUIRE(chunk_stats->cardinality == chunk_get->Cast<LogicalColumnDataGet>().collection->Count());
	REQUIRE(chunk_stats->cardinality > 0);

	auto explain_plan = connection.ExtractPlan("EXPLAIN SELECT 42");
	REQUIRE(explain_plan->type == LogicalOperatorType::LOGICAL_EXPLAIN);
	auto unsupported_child = make_uniq<UnsupportedOutputOperator>(TableIndex(50));
	unsupported_child->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(40)));
	explain_plan->children[0] = std::move(unsupported_child);
	RelationStatsExtractor explain_extractor(*connection.context);
	auto explain_stats = explain_extractor.Extract(*explain_plan);
	REQUIRE(explain_stats);
	REQUIRE(explain_stats->MatchesBindings(explain_plan->GetColumnBindings()));
	REQUIRE(explain_stats->cardinality == 3);
}

TEST_CASE("Relation statistics extraction rebinds CTE outputs", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto definition = CreateProjectedDummy(TableIndex(10), TableIndex(20));
	auto cte_index = TableIndex(30);
	LogicalCTERef cte_ref(TableIndex(40), cte_index, {LogicalType::INTEGER}, {Identifier("i")});
	RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		return index == cte_index ? optional_ptr<LogicalOperator>(*definition) : nullptr;
	});

	auto stats = extractor.Extract(cte_ref);
	REQUIRE(stats);
	REQUIRE(stats->MatchesBindings(cte_ref.GetColumnBindings()));
	REQUIRE(stats->columns[0].binding == ColumnBinding(TableIndex(40), ProjectionIndex(0)));
	REQUIRE(extractor.ExtractedOperatorCount() == 3);
	REQUIRE(extractor.Extract(cte_ref) == stats);
	REQUIRE(extractor.ExtractedOperatorCount() == 3);
	LogicalCTERef second_ref(TableIndex(41), cte_index, {LogicalType::INTEGER}, {Identifier("i")});
	auto second_stats = extractor.Extract(second_ref);
	REQUIRE(second_stats);
	REQUIRE(second_stats->MatchesBindings(second_ref.GetColumnBindings()));
	REQUIRE(extractor.ExtractedOperatorCount() == 4);
}

TEST_CASE("Linear recursive CTE statistics preserve their output layout", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(R"(
		WITH RECURSIVE values(i) AS (
			SELECT 1
			UNION ALL
			SELECT i + 1 FROM values WHERE i < 10
		)
		SELECT i FROM values
	)");
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	auto plan = std::move(planner.plan);
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats stats;
	plan = optimizer.Optimize(std::move(plan), stats);

	REQUIRE(stats.stats_initialized);
	REQUIRE(stats.cardinality == 1001);
	REQUIRE(stats.MatchesBindings(plan->GetColumnBindings()));
	REQUIRE(stats.columns.size() == 1);
	REQUIRE(stats.columns[0].distinct_count.distinct_count == stats.cardinality);
	REQUIRE(stats.columns[0].distinct_count.source == DistinctCountSource::CARDINALITY);
	connection.Rollback();
}

TEST_CASE("Recursive CTE join terms use fixpoint cardinality fallbacks", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(R"(
		WITH RECURSIVE values(i) AS (
			SELECT 1
			UNION ALL
			SELECT i + 1 FROM values, (VALUES (1)) extra(j) WHERE i < 10
		)
		SELECT i FROM values
	)");
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	auto plan = std::move(planner.plan);
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats stats;
	plan = optimizer.Optimize(std::move(plan), stats);

	REQUIRE(stats.stats_initialized);
	REQUIRE(stats.cardinality == 1001);
	REQUIRE(stats.MatchesBindings(plan->GetColumnBindings()));
	REQUIRE(stats.columns.size() == 1);
	REQUIRE(stats.columns[0].distinct_count.distinct_count == stats.cardinality);
	REQUIRE(stats.columns[0].distinct_count.source == DistinctCountSource::CARDINALITY);
	connection.Rollback();
}

TEST_CASE("Relation statistics extraction rejects recurring and cyclic CTEs", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto cte_index = TableIndex(30);
	LogicalCTERef recurring(TableIndex(40), cte_index, {LogicalType::INTEGER}, {Identifier("i")}, true);
	RelationStatsExtractor recurring_extractor(*connection.context,
	                                           [&](TableIndex) -> optional_ptr<LogicalOperator> { return recurring; });
	REQUIRE_FALSE(recurring_extractor.Extract(recurring));

	LogicalCTERef cyclic(TableIndex(50), cte_index, {LogicalType::INTEGER}, {Identifier("i")});
	RelationStatsExtractor cyclic_extractor(*connection.context,
	                                        [&](TableIndex) -> optional_ptr<LogicalOperator> { return cyclic; });
	REQUIRE_FALSE(cyclic_extractor.Extract(cyclic));
	REQUIRE(cyclic_extractor.ExtractedOperatorCount() == 1);
}
