#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/filter_info.hpp"
#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_operator.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

#include <cmath>
#include <limits>

using namespace duckdb;

static RelationStats CreateStats(const vector<ColumnBinding> &bindings, const vector<idx_t> &distinct_counts,
                                 idx_t cardinality) {
	REQUIRE(bindings.size() == distinct_counts.size());
	RelationStats result;
	result.cardinality = cardinality;
	result.max_cardinality = optional_idx(cardinality);
	result.stats_initialized = true;
	for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
		auto domain = DistinctCount(distinct_counts[column_idx], DistinctCountSource::EXACT);
		result.columns.emplace_back(bindings[column_idx], domain, domain,
		                            Identifier("column_" + to_string(column_idx)));
	}
	result.Verify(bindings);
	return result;
}

static JoinOrderOperator &AddOperator(vector<unique_ptr<JoinOrderOperator>> &operators, JoinRelationSetManager &sets,
                                      JoinRelationSet &left, JoinRelationSet &right, JoinOrderOperatorType type) {
	auto op = make_uniq<JoinOrderOperator>(operators.size(), type, left, right, sets.Union(left, right),
	                                       vector<JoinCondition>());
	op->left_total_set = left;
	op->right_total_set = right;
	op->must_apply_as_operator = type != JoinOrderOperatorType::INNER && type != JoinOrderOperatorType::CROSS_PRODUCT;
	operators.push_back(std::move(op));
	return *operators.back();
}

TEST_CASE("INNER join costs distinguish estimates below one", "[optimizer][relation_statistics]") {
	for (double rows : {0.25, 0.75}) {
		JoinRelationSetManager sets;
		auto &left = sets.GetJoinRelation(RelationIndex(0));
		auto &right = sets.GetJoinRelation(RelationIndex(1));
		auto &output = sets.Union(left, right);
		vector<unique_ptr<JoinOrderOperator>> operators;
		auto &inner = AddOperator(operators, sets, left, right, JoinOrderOperatorType::INNER);
		JoinPredicateModel predicates;
		CardinalityEstimator estimator(sets, predicates);
		auto left_stats = CreateStats({ColumnBinding(TableIndex(0), ProjectionIndex(0))}, {1}, 1);
		left_stats.cardinality = rows;
		left_stats.columns[0].current_domain.distinct_count = rows;
		estimator.Initialize({left_stats, CreateStats({ColumnBinding(TableIndex(1), ProjectionIndex(0))}, {1}, 1)},
		                     operators);
		CostModel costs(estimator);
		DPJoinNode lhs(left), rhs(right);
		lhs.cost = 2;
		rhs.cost = 3;
		REQUIRE(costs.ComputeCost(lhs, rhs, &inner, output) == Approx(5 + rows));
		REQUIRE(costs.ComputeCost(lhs, rhs, nullptr, output) == Approx(5 + rows));
		REQUIRE(costs.ComputeCostLowerBound(lhs, rhs, rows) == Approx(5 + rows));
	}
}

TEST_CASE("MARK boundaries preserve the left population and publish their marker", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t rhs_rows : {idx_t(0), idx_t(3)}) {
		JoinRelationSetManager sets;
		auto &left = sets.GetJoinRelation(RelationIndex(0));
		auto &right = sets.GetJoinRelation(RelationIndex(1));
		auto &output = sets.Union(left, right);
		vector<unique_ptr<JoinOrderOperator>> operators;
		auto &mark = AddOperator(operators, sets, left, right, JoinOrderOperatorType::MARK);
		auto left_binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
		auto right_binding = ColumnBinding(TableIndex(1), ProjectionIndex(0));
		auto marker_binding = ColumnBinding(TableIndex(1), ProjectionIndex(1));
		auto left_stats = CreateStats({left_binding}, {1}, 1);
		left_stats.cardinality = 0.25;
		left_stats.row_retention = 0.25;
		left_stats.columns[0].current_domain.distinct_count = 0.25;
		left_stats.columns[0].semi_anti_join_domain_evidence.MarkUnique();
		auto right_stats = CreateStats({right_binding}, {rhs_rows}, rhs_rows);
		JoinPredicateModel predicates;
		CardinalityEstimator estimator(sets, predicates);
		estimator.Initialize({left_stats, right_stats}, operators);
		auto projected = estimator.ProjectStats(output, {left_binding, marker_binding});
		REQUIRE(projected);
		REQUIRE(projected->cardinality == left_stats.cardinality);
		REQUIRE(projected->row_retention == left_stats.row_retention);
		REQUIRE(projected->max_cardinality.GetIndex() == 1);
		REQUIRE_FALSE(projected->IsEmpty());
		REQUIRE(projected->columns[1].total_domain.distinct_count == 3);
		REQUIRE(projected->columns[1].current_domain.distinct_count == 0.25);
		REQUIRE_FALSE(estimator.ProjectStats(output, {right_binding}));

		LogicalComparisonJoin logical_mark(JoinType::MARK);
		logical_mark.mark_index = TableIndex(2);
		logical_mark.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(0)));
		logical_mark.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(1)));
		auto extracted = RelationStatisticsHelper::ExtractOperatorStats(logical_mark, *connection.context,
		                                                                {left_stats, right_stats});
		REQUIRE(extracted);
		REQUIRE(extracted->cardinality == projected->cardinality);
		REQUIRE(extracted->row_retention == projected->row_retention);
		REQUIRE(extracted->max_cardinality.GetIndex() == projected->max_cardinality.GetIndex());
		REQUIRE(extracted->columns.size() == projected->columns.size());
		for (idx_t i = 0; i < extracted->columns.size(); i++) {
			REQUIRE(extracted->columns[i].total_domain.distinct_count ==
			        projected->columns[i].total_domain.distinct_count);
			REQUIRE(extracted->columns[i].current_domain.distinct_count ==
			        projected->columns[i].current_domain.distinct_count);
		}
		auto extracted_evidence = extracted->columns[0].GetSupportedSemiAntiDomainSize();
		auto projected_evidence = projected->columns[0].GetSupportedSemiAntiDomainSize();
		REQUIRE(extracted_evidence);
		REQUIRE(projected_evidence);
		REQUIRE(*extracted_evidence == *projected_evidence);

		CostModel costs(estimator);
		DPJoinNode lhs(left), rhs(right);
		lhs.cost = 2;
		rhs.cost = 3;
		REQUIRE(costs.ComputeCostLowerBound(lhs, rhs, 0.25) <= costs.ComputeCost(lhs, rhs, &mark, output));
	}
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

TEST_CASE("An internal single-input filter survives join reconstruction",
          "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	auto cte = TableIndex(20);
	auto input = TableIndex(21);
	auto cte_reference =
	    make_uniq<LogicalCTERef>(input, cte, vector<LogicalType> {LogicalType::INTEGER}, vector<Identifier> {"k"});
	unique_ptr<Expression> predicate_input;
	SECTION("Column binding") {
		predicate_input =
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(input, ProjectionIndex(0)));
	}
	SECTION("Positional binding") {
		predicate_input = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0);
	}
	auto filter = make_uniq<LogicalFilter>(
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(predicate_input),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(1))));
	filter->children.push_back(
	    make_uniq<LogicalCrossProduct>(std::move(cte_reference), CreateProjectedDummy(TableIndex(30), TableIndex(31))));
	unique_ptr<LogicalOperator> plan =
	    make_uniq<LogicalCrossProduct>(std::move(filter), CreateProjectedDummy(TableIndex(40), TableIndex(41)));
	JoinOrderOptimizer optimizer(*connection.context);
	optimizer.AddMaterializedCTEStats(cte, CreateStats({ColumnBinding(cte, ProjectionIndex(0))}, {2}, 2));
	plan = optimizer.Optimize(std::move(plan));
	idx_t expressions = 0;
	vector<reference<LogicalOperator>> pending {*plan};
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			expressions += op.expressions.size();
		}
		for (auto &child : op.children) {
			pending.push_back(*child);
		}
	}
	REQUIRE(expressions == 1);
	CHECK(plan->estimated_cardinality == 1);
}

TEST_CASE("A positional filter retains its column when a LEFT input is reordered",
          "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	JoinOrderOptimizer optimizer(*connection.context);
	vector<unique_ptr<LogicalOperator>> inputs;
	for (idx_t side = 0; side < 4; side++) {
		auto definition = TableIndex(100 + side);
		auto input = TableIndex(10 * (side + 1));
		optimizer.AddMaterializedCTEStats(
		    definition, CreateStats({ColumnBinding(definition, ProjectionIndex(0))}, {1}, side == 2 ? 100 : 1));
		inputs.push_back(make_uniq<LogicalCTERef>(input, definition, vector<LogicalType> {LogicalType::INTEGER},
		                                          vector<Identifier> {"k"}));
	}
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	join->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(10), ProjectionIndex(0))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(20), ProjectionIndex(0))),
	    ExpressionType::COMPARE_EQUAL);
	join->children.push_back(std::move(inputs[0]));
	join->children.push_back(make_uniq<LogicalCrossProduct>(
	    make_uniq<LogicalCrossProduct>(std::move(inputs[1]), std::move(inputs[2])), std::move(inputs[3])));
	auto filter = make_uniq<LogicalFilter>(BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 2),
	    make_uniq<BoundConstantExpression>(Value::INTEGER(7))));
	filter->children.push_back(std::move(join));
	unique_ptr<LogicalOperator> plan = std::move(filter);
	plan->ResolveOperatorTypes();
	auto expected = ColumnBinding(TableIndex(30), ProjectionIndex(0));
	REQUIRE(plan->GetColumnBindings()[2] == expected);
	plan = optimizer.Optimize(std::move(plan));
	REQUIRE(plan->GetColumnBindings()[2] != expected);
	idx_t found = 0;
	vector<reference<LogicalOperator>> pending {*plan};
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			for (auto &expression : op.expressions) {
				auto &predicate = expression->Cast<BoundFunctionExpression>();
				auto &left = BoundComparisonExpression::Left(predicate);
				REQUIRE(left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF);
				REQUIRE(left.Cast<BoundColumnRefExpression>().Binding() == expected);
				found++;
			}
		}
		for (auto &child : op.children) {
			pending.push_back(*child);
		}
	}
	REQUIRE(found == 1);
}

TEST_CASE("Join region filters have one statistics owner", "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	for (bool recursive : {false, true}) {
		for (bool nested : {false, true}) {
			for (bool request_stats : {false, true}) {
				for (idx_t wrapper = 0; wrapper < 8; wrapper++) {
					if (nested && (wrapper == 1 || wrapper == 2)) {
						// LIMIT controls exercise the existing root-ancestor handoff.
						continue;
					}
					auto cte_index = TableIndex(10);
					auto reference_index = TableIndex(20);
					auto seed = CreateStats({ColumnBinding(cte_index, ProjectionIndex(0))}, {1}, 1);
					auto reference =
					    make_uniq<LogicalCTERef>(reference_index, cte_index, vector<LogicalType> {LogicalType::INTEGER},
					                             vector<Identifier> {"i"});
					auto filter = make_uniq<LogicalFilter>(BoundComparisonExpression::Create(
					    ExpressionType::COMPARE_LESSTHAN,
					    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
					                                        ColumnBinding(reference_index, ProjectionIndex(0))),
					    make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
					filter->children.push_back(std::move(reference));
					unique_ptr<LogicalOperator> plan = make_uniq<LogicalCrossProduct>(
					    std::move(filter), CreateProjectedDummy(TableIndex(30), TableIndex(31)));
					auto join_before = plan.get();
					double expected = 0.2;
					bool empty = wrapper == 2 || wrapper == 7;
					if (wrapper == 1 || wrapper == 2) {
						auto limit = make_uniq<LogicalLimit>(wrapper == 1 ? BoundLimitNode::ConstantPercentage(50)
						                                                  : BoundLimitNode::ConstantValue(0),
						                                     BoundLimitNode());
						limit->children.push_back(std::move(plan));
						plan = std::move(limit);
						expected = wrapper == 1 ? 0.1 : 0;
					} else if (wrapper >= 3) {
						auto root_filter = make_uniq<LogicalFilter>();
						if (wrapper == 7) {
							root_filter->expressions.push_back(
							    make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
							expected = 0;
						} else {
							const bool cross = wrapper == 4 || wrapper == 5;
							unique_ptr<Expression> rhs =
							    cross ? unique_ptr<Expression>(make_uniq<BoundColumnRefExpression>(
							                LogicalType::INTEGER, ColumnBinding(TableIndex(31), ProjectionIndex(0))))
							          : unique_ptr<Expression>(
							                make_uniq<BoundConstantExpression>(Value::INTEGER(wrapper == 6 ? 0 : 50)));
							root_filter->expressions.push_back(BoundComparisonExpression::Create(
							    wrapper == 6 ? ExpressionType::COMPARE_GREATERTHAN : ExpressionType::COMPARE_LESSTHAN,
							    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
							                                        ColumnBinding(reference_index, ProjectionIndex(0))),
							    std::move(rhs)));
							expected = cross ? 0.1 : 0.04;
							if (wrapper == 3 || wrapper == 5) {
								root_filter->expressions.push_back(BoundComparisonExpression::Create(
								    ExpressionType::COMPARE_LESSTHAN,
								    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0),
								    make_uniq<BoundConstantExpression>(Value::INTEGER(50))));
								if (cross) {
									expected *= 0.2;
								}
							}
						}
						root_filter->projection_map = {ProjectionIndex(1), ProjectionIndex(0), ProjectionIndex(1)};
						root_filter->children.push_back(std::move(plan));
						plan = std::move(root_filter);
					}
					if (nested) {
						plan = make_uniq<LogicalCrossProduct>(std::move(plan),
						                                      CreateProjectedDummy(TableIndex(40), TableIndex(41)));
					}
					plan->ResolveOperatorTypes();
					auto bindings = plan->GetColumnBindings();
					plan->SetEstimatedCardinality(9876);
					auto retained = plan.get();
					if (!recursive && request_stats) {
						auto definition = CreateProjectedDummy(TableIndex(9), cte_index);
						RelationStatsExtractor extractor(*connection.context,
						                                 [&](TableIndex index) -> optional_ptr<LogicalOperator> {
							                                 return index == cte_index ? definition.get() : nullptr;
						                                 });
						auto extracted = extractor.Extract(*plan);
						INFO("read-only nested=" << nested << " wrapper=" << wrapper);
						REQUIRE(extracted);
						REQUIRE(extracted->MatchesBindings(bindings));
						REQUIRE(extracted->cardinality.GetDouble() == Approx(expected));
						REQUIRE(extracted->IsEmpty() == empty);
						REQUIRE(plan->estimated_cardinality == 9876);
					}
					JoinOrderOptimizer optimizer(*connection.context);
					optimizer.AddMaterializedCTEStats(cte_index, std::move(seed));
					if (recursive) {
						optimizer.recursive_cte_indexes.insert(cte_index);
					}
					RelationStats stats;
					plan = optimizer.Optimize(std::move(plan), request_stats ? &stats : nullptr);
					INFO("recursive=" << recursive << " nested=" << nested << " wrapper=" << wrapper);
					REQUIRE(plan->estimated_cardinality == (empty ? 0 : 1));
					if (recursive && !nested) {
						REQUIRE(plan.get() == retained);
						auto &join_after = wrapper == 0 ? *plan : *plan->children[0];
						REQUIRE(&join_after == join_before);
						REQUIRE(join_after.children[0]->type == LogicalOperatorType::LOGICAL_FILTER);
						if (wrapper >= 3) {
							REQUIRE(plan->expressions.size() == (wrapper == 3 || wrapper == 5 ? 2 : 1));
						}
					}
					if (request_stats) {
						REQUIRE(stats.stats_initialized);
						REQUIRE(stats.MatchesBindings(plan->GetColumnBindings()));
						REQUIRE(stats.cardinality.GetDouble() == Approx(expected));
						REQUIRE(stats.IsEmpty() == empty);
						if (wrapper >= 3 && !nested) {
							REQUIRE(stats.MatchesBindings(bindings));
						}
					}
				}
			}
		}
	}
}

TEST_CASE("Multiplicity-one domain retention preserves its row estimate",
          "[optimizer][relation_statistics][current_domain_precision]") {
	for (auto source : {DistinctCountSource::EXACT, DistinctCountSource::HLL, DistinctCountSource::CARDINALITY}) {
		for (double input : {1.0, 1000.0, 1e200}) {
			for (double fraction : {0.1, 1e-100}) {
				for (double domain_scale : {1.0, 2.0}) {
					CardinalityValue rows(input);
					auto output = rows * CardinalityValue(fraction);
					DistinctCount domain(rows * CardinalityValue(domain_scale), source);
					auto retained = RelationStatisticsHelper::EstimateCurrentDomain(domain, rows, output);
					REQUIRE(retained.distinct_count == output);
					REQUIRE(retained.source == DistinctCountSource::CARDINALITY);
				}
			}
		}
		// With two occurrences per value, retention is 1 - (0.9 * 0.9), not the retained row count.
		auto repeated = RelationStatisticsHelper::EstimateCurrentDomain(DistinctCount(500, source), 1000, 100);
		REQUIRE(repeated.distinct_count.GetDouble() == Approx(95));
		REQUIRE(repeated.source == DistinctCountSource::CARDINALITY);
	}
}

TEST_CASE("Independent join inputs already include their root filters",
          "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	for (auto type : {JoinType::SEMI, JoinType::ANTI, JoinType::LEFT}) {
		auto cte_index = TableIndex(10);
		auto rhs_binding = ColumnBinding(TableIndex(20), ProjectionIndex(0));
		auto reference = make_uniq<LogicalCTERef>(rhs_binding.table_index, cte_index,
		                                          vector<LogicalType> {LogicalType::INTEGER}, vector<Identifier> {"i"});
		auto filter = make_uniq<LogicalFilter>(BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_LESSTHAN, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, rhs_binding),
		    make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
		filter->children.push_back(std::move(reference));
		auto join = make_uniq<LogicalComparisonJoin>(type);
		join->children.push_back(CreateProjectedDummy(TableIndex(30), TableIndex(31)));
		join->children.push_back(std::move(filter));
		join->conditions.emplace_back(make_uniq<BoundColumnRefExpression>(
		                                  LogicalType::INTEGER, ColumnBinding(TableIndex(31), ProjectionIndex(0))),
		                              make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, rhs_binding),
		                              ExpressionType::COMPARE_EQUAL);
		join->ResolveOperatorTypes();
		JoinOrderOptimizer optimizer(*connection.context);
		optimizer.AddMaterializedCTEStats(cte_index,
		                                  CreateStats({ColumnBinding(cte_index, ProjectionIndex(0))}, {1}, 1));
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.Build(optimizer, *join));
		REQUIRE(graph.relation_manager.NumRelations() == 2);
		REQUIRE(graph.relation_manager.GetRelation(RelationIndex(1)).stats.cardinality.GetDouble() == Approx(0.2));
	}
}

TEST_CASE("ON predicates do not own equal WHERE expressions", "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t variant = 0; variant < 3; variant++) {
		const bool nullable_side = variant == 1;
		const bool anti = variant == 2;
		auto lhs = ColumnBinding(TableIndex(20), ProjectionIndex(0));
		auto rhs = ColumnBinding(TableIndex(21), ProjectionIndex(0));
		auto join = make_uniq<LogicalComparisonJoin>(anti ? JoinType::ANTI : JoinType::LEFT);
		for (idx_t side = 0; side < 2; side++) {
			join->children.push_back(make_uniq<LogicalCTERef>(TableIndex(20 + side), TableIndex(10 + side),
			                                                  vector<LogicalType> {LogicalType::INTEGER},
			                                                  vector<Identifier> {"i"}));
		}
		join->conditions.emplace_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, lhs),
		                              make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, rhs),
		                              ExpressionType::COMPARE_EQUAL);
		join->conditions.emplace_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, lhs),
		                              make_uniq<BoundConstantExpression>(Value::INTEGER(5)),
		                              ExpressionType::COMPARE_LESSTHAN);
		auto filter = make_uniq<LogicalFilter>(BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_LESSTHAN,
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, nullable_side ? rhs : lhs),
		    make_uniq<BoundConstantExpression>(Value::INTEGER(5))));
		filter->children.push_back(std::move(join));
		unique_ptr<LogicalOperator> plan = std::move(filter);
		plan->ResolveOperatorTypes();
		JoinOrderOptimizer optimizer(*connection.context);
		for (idx_t side = 0; side < 2; side++) {
			auto index = TableIndex(10 + side);
			optimizer.AddMaterializedCTEStats(index, CreateStats({ColumnBinding(index, ProjectionIndex(0))}, {10}, 10));
		}
		RelationStats stats;
		plan = optimizer.Optimize(std::move(plan), stats);
		REQUIRE(stats.stats_initialized);
		REQUIRE(stats.MatchesBindings(plan->GetColumnBindings()));
		// Residual ANTI conditions retain the existing .2 fallback.
		// Its separate WHERE contributes another .2, without being consumed by ON.
		REQUIRE(stats.cardinality.GetDouble() == Approx(anti ? 0.4 : 2));
		REQUIRE(plan->estimated_cardinality == (anti ? 1 : 2));
	}
}

TEST_CASE("Incomplete retained regions do not prepare a partial predicate model",
          "[optimizer][relation_statistics][retained_region]") {
	DuckDB db;
	Connection connection(db);
	auto options = make_uniq<SampleOptions>();
	options->sample_size = Value::DOUBLE(50);
	options->is_percentage = true;
	options->method = SampleMethod::BERNOULLI_SAMPLE;
	auto sample = make_uniq<LogicalSample>(std::move(options), CreateProjectedDummy(TableIndex(50), TableIndex(51)));
	auto pair = make_uniq<LogicalCrossProduct>(CreateProjectedDummy(TableIndex(10), TableIndex(11)),
	                                           CreateProjectedDummy(TableIndex(30), TableIndex(31)));
	unique_ptr<LogicalOperator> plan = make_uniq<LogicalCrossProduct>(std::move(pair), std::move(sample));
	plan->ResolveOperatorTypes();
	auto retained = plan.get();
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats stats;
	plan = optimizer.Optimize(std::move(plan), stats);
	REQUIRE(plan.get() == retained);
	REQUIRE_FALSE(stats.stats_initialized);
	REQUIRE(plan->children[1]->type == LogicalOperatorType::LOGICAL_SAMPLE);
}

TEST_CASE("Composite SEMI domains retain their correlation constraints", "[optimizer][relation_statistics]") {
	auto left_binding = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto second_left_binding = ColumnBinding(TableIndex(10), ProjectionIndex(1));
	auto right_binding = ColumnBinding(TableIndex(20), ProjectionIndex(0));
	auto second_right_binding = ColumnBinding(TableIndex(20), ProjectionIndex(1));
	vector<SemiAntiJoinDomain> composite_domains {{1000, 100, left_binding, right_binding},
	                                              {1000, 100, second_left_binding, second_right_binding}};
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(1000, 1000, 0.5, JoinType::SEMI,
	                                                                  composite_domains, false)
	            .cardinality == 100);
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(1000, 1000, 0.5, JoinType::SEMI,
	                                                                  composite_domains, true)
	            .cardinality == 200);

	vector<SemiAntiJoinDomain> shared_endpoint_domains {{1000, 100, left_binding, right_binding},
	                                                    {1000, 100, left_binding, second_right_binding}};
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(1000, 1000, 0.5, JoinType::SEMI,
	                                                                  shared_endpoint_domains, false)
	            .cardinality == 10);
	vector<SemiAntiJoinDomain> full_coverage {{1000, 1000, left_binding, right_binding}};
	REQUIRE(
	    RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(1000, 1000, 1, JoinType::ANTI, full_coverage, false)
	        .cardinality == 0);
}

TEST_CASE("Nested empty SEMI remains exact in logical estimates", "[optimizer][relation_statistics]") {
	for (auto outer_join_type : {JoinType::SEMI, JoinType::ANTI}) {
		JoinRelationSetManager set_manager;
		auto &left_set = set_manager.GetJoinRelation(RelationIndex(0));
		auto &right_set = set_manager.GetJoinRelation(RelationIndex(1));
		auto &empty_set = set_manager.GetJoinRelation(RelationIndex(2));
		auto &inner_set = set_manager.Union(right_set, empty_set);
		auto &root_set = set_manager.Union(left_set, inner_set);
		auto left_binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
		auto right_binding = ColumnBinding(TableIndex(1), ProjectionIndex(0));
		auto empty_binding = ColumnBinding(TableIndex(2), ProjectionIndex(0));

		auto inner_comparison = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right_binding),
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, empty_binding));
		FilterInfo inner_filter(std::move(inner_comparison), inner_set, 0, JoinType::SEMI);
		inner_filter.SetLeftSet(right_set);
		inner_filter.SetRightSet(empty_set);
		inner_filter.left_binding = right_binding;
		inner_filter.right_binding = empty_binding;
		inner_filter.source_operator_index = optional_idx(0);
		inner_filter.semantic_left_set = right_set;
		inner_filter.semantic_right_set = empty_set;

		auto outer_comparison = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left_binding),
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right_binding));
		FilterInfo outer_filter(std::move(outer_comparison), root_set, 1, outer_join_type);
		outer_filter.SetLeftSet(left_set);
		outer_filter.SetRightSet(inner_set);
		outer_filter.left_binding = left_binding;
		outer_filter.right_binding = right_binding;
		outer_filter.source_operator_index = optional_idx(1);
		outer_filter.semantic_left_set = left_set;
		outer_filter.semantic_right_set = inner_set;

		JoinPredicateModel predicate_model;
		predicate_model.RegisterPredicate(inner_filter, JoinPredicateClass::SEMI_ANTI_JOIN, right_binding,
		                                  empty_binding);
		predicate_model.RegisterPredicate(outer_filter, JoinPredicateClass::SEMI_ANTI_JOIN, left_binding,
		                                  right_binding);
		CardinalityEstimator estimator(set_manager, predicate_model);

		auto left_stats = CreateStats({left_binding}, {1000}, 1000);
		auto right_stats = CreateStats({right_binding}, {100}, 100);
		auto empty_stats = CreateStats({empty_binding}, {0}, 0);
		vector<unique_ptr<JoinOrderOperator>> operators;
		AddOperator(operators, set_manager, right_set, empty_set, JoinOrderOperatorType::SEMI);
		auto type = outer_join_type == JoinType::SEMI ? JoinOrderOperatorType::SEMI : JoinOrderOperatorType::ANTI;
		AddOperator(operators, set_manager, left_set, inner_set, type);
		estimator.Initialize({left_stats, right_stats, empty_stats}, operators);
		auto inner_rows = estimator.EstimateCardinality(inner_set);
		auto outer_rows = estimator.EstimateCardinality(root_set);
		REQUIRE(outer_rows.GetDouble() == (outer_join_type == JoinType::SEMI ? 0 : 1000));
		REQUIRE(inner_rows.IsZero());
	}
}

TEST_CASE("Empty multi-relation preserved SEMI remains exact", "[optimizer][relation_statistics]") {
	JoinRelationSetManager set_manager;
	auto &nonempty_set = set_manager.GetJoinRelation(RelationIndex(0));
	auto &empty_set = set_manager.GetJoinRelation(RelationIndex(1));
	auto &rhs_set = set_manager.GetJoinRelation(RelationIndex(2));
	auto &preserved_set = set_manager.Union(nonempty_set, empty_set);
	auto &root_set = set_manager.Union(preserved_set, rhs_set);
	auto nonempty_binding = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	auto empty_binding = ColumnBinding(TableIndex(1), ProjectionIndex(0));
	auto rhs_binding = ColumnBinding(TableIndex(2), ProjectionIndex(0));

	auto comparison = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, nonempty_binding),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, rhs_binding));
	FilterInfo filter(std::move(comparison), root_set, 0, JoinType::SEMI);
	filter.SetLeftSet(preserved_set);
	filter.SetRightSet(rhs_set);
	filter.left_binding = nonempty_binding;
	filter.right_binding = rhs_binding;
	filter.source_operator_index = optional_idx(0);
	filter.semantic_left_set = preserved_set;
	filter.semantic_right_set = rhs_set;

	JoinPredicateModel predicate_model;
	predicate_model.RegisterPredicate(filter, JoinPredicateClass::SEMI_ANTI_JOIN, nonempty_binding, rhs_binding);
	CardinalityEstimator estimator(set_manager, predicate_model);

	auto nonempty_stats = CreateStats({nonempty_binding}, {100}, 100);
	auto empty_stats = CreateStats({empty_binding}, {0}, 0);
	auto rhs_stats = CreateStats({rhs_binding}, {100}, 100);
	vector<unique_ptr<JoinOrderOperator>> operators;
	AddOperator(operators, set_manager, preserved_set, rhs_set, JoinOrderOperatorType::SEMI);
	estimator.Initialize({nonempty_stats, empty_stats, rhs_stats}, operators);
	REQUIRE(estimator.EstimateCardinality(root_set).IsZero());
	REQUIRE(estimator.EstimateCardinality(preserved_set).IsZero());
}

static unique_ptr<Expression> CreateJoinComparison(ColumnBinding left, ColumnBinding right, ExpressionType type) {
	return BoundComparisonExpression::Create(type, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left),
	                                         make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right));
}

TEST_CASE("Equivalent inner regions project the same statistics after either split",
          "[optimizer][relation_statistics]") {
	for (bool right_first : {false, true}) {
		JoinRelationSetManager sets;
		JoinPredicateModel model;
		vector<unique_ptr<FilterInfo>> filters;
		vector<ColumnBinding> bindings;
		vector<RelationStats> inputs;
		JoinEqualityClass group;
		group.index = 0;
		for (idx_t i = 0; i < 3; i++) {
			auto binding = ColumnBinding(TableIndex(i), ProjectionIndex(0));
			bindings.push_back(binding);
			inputs.push_back(CreateStats({binding}, {10}, 10 * (i + 1)));
			group.columns.insert(binding);
			group.relations.insert(RelationIndex(i));
			if (i == 0) {
				continue;
			}
			auto &left = sets.GetJoinRelation(RelationIndex(i - 1));
			auto &right = sets.GetJoinRelation(RelationIndex(i));
			auto filter =
			    make_uniq<FilterInfo>(CreateJoinComparison(bindings[i - 1], binding, ExpressionType::COMPARE_EQUAL),
			                          sets.Union(left, right), i - 1);
			filter->SetLeftSet(left);
			filter->SetRightSet(right);
			filter->left_binding = bindings[i - 1];
			filter->right_binding = binding;
			auto &predicate =
			    model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, bindings[i - 1], binding);
			predicate.SetEqualityClassIndex(optional_idx(0));
			group.edges.emplace_back(predicate, RelationIndex(i - 1), RelationIndex(i), bindings[i - 1], binding);
			filters.push_back(std::move(filter));
		}
		model.AddEqualityClass(std::move(group));
		CardinalityEstimator estimator(sets, model);
		estimator.Initialize(inputs);
		auto &a = sets.GetJoinRelation(RelationIndex(0));
		auto &b = sets.GetJoinRelation(RelationIndex(1));
		auto &c = sets.GetJoinRelation(RelationIndex(2));
		auto &ab = sets.Union(a, b);
		auto &bc = sets.Union(b, c);
		auto &root = sets.Union(ab, c);
		REQUIRE(estimator.EstimateCardinality(right_first ? bc : ab) == (right_first ? 60 : 20));
		auto before = estimator.ProjectStats(root, bindings);
		REQUIRE(before);
		REQUIRE(before->cardinality == 60);
		REQUIRE(estimator.EstimateCardinality(right_first ? ab : bc) == (right_first ? 20 : 60));
		auto after = estimator.ProjectStats(root, bindings);
		REQUIRE(after);
		REQUIRE(after->cardinality == before->cardinality);
		for (idx_t i = 0; i < bindings.size(); i++) {
			REQUIRE(before->columns[i].current_domain.distinct_count == 10);
			REQUIRE(after->columns[i].current_domain.distinct_count ==
			        before->columns[i].current_domain.distinct_count);
		}
	}
}

TEST_CASE("Join source populations survive changes in relation cardinality", "[optimizer][relation_statistics]") {
	for (auto current_rows : {idx_t(100000), idx_t(8006)}) {
		for (auto weak_source : {DistinctCountSource::CARDINALITY, DistinctCountSource::MIN_MAX}) {
			JoinRelationSetManager sets;
			JoinPredicateModel model;
			auto left = ColumnBinding(TableIndex(0), ProjectionIndex(0));
			auto right = ColumnBinding(TableIndex(1), ProjectionIndex(0));
			auto &lhs = sets.GetJoinRelation(RelationIndex(0));
			auto &rhs = sets.GetJoinRelation(RelationIndex(1));
			auto &joined = sets.Union(lhs, rhs);
			FilterInfo filter(CreateJoinComparison(left, right, ExpressionType::COMPARE_EQUAL), joined, 0);
			filter.SetLeftSet(lhs);
			filter.SetRightSet(rhs);
			filter.left_binding = left;
			filter.right_binding = right;
			auto &predicate = model.RegisterPredicate(filter, JoinPredicateClass::INNER_EQUALITY, left, right);
			predicate.SetEqualityClassIndex(optional_idx(0));
			JoinEqualityClass group;
			group.index = 0;
			group.columns = {left, right};
			group.relations = {RelationIndex(0), RelationIndex(1)};
			group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(1), left, right);
			model.AddEqualityClass(std::move(group));
			auto left_stats = CreateStats({left}, {50}, current_rows);
			auto right_stats = CreateStats({right}, {10000}, 10000);
			right_stats.columns[0].total_domain = DistinctCount(20000, weak_source);
			join_column_sources_t sources;
			sources.emplace(left, JoinColumnSources({ColumnBinding(TableIndex(10), ProjectionIndex(0)), 100000, 100000,
			                                         left_stats.columns[0].total_domain}));
			sources.emplace(right, JoinColumnSources({ColumnBinding(TableIndex(20), ProjectionIndex(0)), 20000, 10000,
			                                          right_stats.columns[0].total_domain}));
			sources.at(right).equivalents.push_back({ColumnBinding(TableIndex(30), ProjectionIndex(0)), 200, 100,
			                                         DistinctCount(200, DistinctCountSource::MIN_MAX)});
			CardinalityEstimator estimator(sets, model);
			estimator.Initialize({left_stats, right_stats}, {}, sources);
			auto expected = weak_source == DistinctCountSource::MIN_MAX ? current_rows : current_rows * 200;
			REQUIRE(estimator.EstimateCardinality(joined).GetIndex() == expected);
		}
	}
}

TEST_CASE("Synthetic relation rows do not replace a base join domain", "[optimizer][relation_statistics]") {
	for (auto synthetic_rows : {idx_t(6), idx_t(600)}) {
		JoinRelationSetManager sets;
		JoinPredicateModel model;
		vector<ColumnBinding> bindings;
		vector<RelationStats> stats;
		join_column_sources_t sources;
		JoinEqualityClass group;
		group.index = 0;
		vector<unique_ptr<FilterInfo>> filters;
		vector<idx_t> populations {100000, 20000, synthetic_rows};
		for (idx_t i = 0; i < populations.size(); i++) {
			bindings.emplace_back(TableIndex(i), ProjectionIndex(0));
			stats.push_back(CreateStats({bindings.back()}, {populations[i]}, populations[i]));
			stats.back().columns[0].total_domain = DistinctCount(populations[i], DistinctCountSource::CARDINALITY);
			group.columns.insert(bindings.back());
			group.relations.insert(RelationIndex(i));
			if (i < 2) {
				sources.emplace(
				    bindings.back(),
				    JoinColumnSources({ColumnBinding(TableIndex(10 + i), ProjectionIndex(0)), populations[i],
				                       populations[i], stats.back().columns[0].total_domain}));
			}
			if (i == 0) {
				continue;
			}
			auto &lhs = sets.GetJoinRelation(RelationIndex(0));
			auto &rhs = sets.GetJoinRelation(RelationIndex(i));
			auto filter = make_uniq<FilterInfo>(
			    CreateJoinComparison(bindings[0], bindings[i], ExpressionType::COMPARE_EQUAL), sets.Union(lhs, rhs), i);
			filter->SetLeftSet(lhs);
			filter->SetRightSet(rhs);
			filter->left_binding = bindings[0];
			filter->right_binding = bindings[i];
			auto &predicate =
			    model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, bindings[0], bindings[i]);
			predicate.SetEqualityClassIndex(optional_idx(0));
			group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(i), bindings[0], bindings[i]);
			filters.push_back(std::move(filter));
		}
		model.AddEqualityClass(std::move(group));
		CardinalityEstimator estimator(sets, model);
		estimator.Initialize(stats, {}, sources);
		auto &base_join = sets.Union(sets.GetJoinRelation(RelationIndex(0)), sets.GetJoinRelation(RelationIndex(1)));
		REQUIRE(estimator.EstimateCardinality(base_join).GetIndex() == 500000);
		auto &full_join = sets.Union(base_join, sets.GetJoinRelation(RelationIndex(2)));
		REQUIRE(estimator.EstimateCardinality(full_join).GetIndex() == 500000);
	}
}

TEST_CASE("Min-max uplift counts sources and rejects unknown reliable populations",
          "[optimizer][relation_statistics]") {
	for (idx_t provenance : {idx_t(0), idx_t(1), idx_t(2)}) {
		// Same source alias, independent source, or reliable statistics behind a boundary.
		JoinRelationSetManager sets;
		JoinPredicateModel model;
		vector<ColumnBinding> bindings;
		vector<RelationStats> stats;
		join_column_sources_t sources;
		JoinEqualityClass group;
		group.index = 0;
		vector<unique_ptr<FilterInfo>> filters;
		for (idx_t i = 0; i < 3; i++) {
			bindings.emplace_back(TableIndex(i), ProjectionIndex(0));
			stats.push_back(CreateStats({bindings.back()}, {i == 2 ? idx_t(1000) : idx_t(50)}, i == 2 ? 1000 : 10000));
			if (i == 2) {
				stats.back().columns[0].total_domain.source = DistinctCountSource::MIN_MAX;
			}
			auto origin = provenance == 0 && i == 1 ? idx_t(10) : 10 + i;
			if (provenance != 2 || i != 1) {
				sources.emplace(bindings.back(),
				                JoinColumnSources({ColumnBinding(TableIndex(origin), ProjectionIndex(0)),
				                                   i == 2 ? idx_t(1000) : idx_t(10000), stats.back().cardinality,
				                                   stats.back().columns[0].total_domain}));
			}
			group.columns.insert(bindings.back());
			group.relations.insert(RelationIndex(i));
			if (i == 0) {
				continue;
			}
			auto &lhs = sets.GetJoinRelation(RelationIndex(0));
			auto &rhs = sets.GetJoinRelation(RelationIndex(i));
			auto filter = make_uniq<FilterInfo>(
			    CreateJoinComparison(bindings[0], bindings[i], ExpressionType::COMPARE_EQUAL), sets.Union(lhs, rhs), i);
			filter->SetLeftSet(lhs);
			filter->SetRightSet(rhs);
			filter->left_binding = bindings[0];
			filter->right_binding = bindings[i];
			auto &predicate =
			    model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, bindings[0], bindings[i]);
			predicate.SetEqualityClassIndex(optional_idx(0));
			group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(i), bindings[0], bindings[i]);
			filters.push_back(std::move(filter));
		}
		model.AddEqualityClass(std::move(group));
		CardinalityEstimator estimator(sets, model);
		estimator.Initialize(stats, {}, sources);
		auto &joined = sets.Union(sets.GetJoinRelation(RelationIndex(0)), sets.GetJoinRelation(RelationIndex(2)));
		REQUIRE(estimator.EstimateCardinality(joined).GetIndex() == (provenance == 0 ? 10000 : 200000));
	}
}

TEST_CASE("Predicate-local fallback domains order competing equality edges", "[optimizer][relation_statistics]") {
	for (bool reverse_classes : {false, true}) {
		for (bool reverse_edges : {false, true}) {
			JoinRelationSetManager sets;
			JoinPredicateModel model;
			vector<RelationStats> stats;
			join_column_sources_t sources;
			vector<unique_ptr<FilterInfo>> filters;
			for (idx_t i = 0; i < 3; i++) {
				auto first = ColumnBinding(TableIndex(i), ProjectionIndex(0));
				auto second = ColumnBinding(TableIndex(i), ProjectionIndex(1));
				idx_t rows = i == 2 ? 10 : 1000;
				stats.push_back(CreateStats({first, second}, {rows, MinValue<idx_t>(rows, 100)}, rows));
				stats.back().columns[0].total_domain.source = DistinctCountSource::CARDINALITY;
				stats.back().columns[1].total_domain = DistinctCount(100, DistinctCountSource::MIN_MAX);
				if (i != 2) {
					sources.emplace(first, JoinColumnSources({first, idx_t(rows), idx_t(rows),
					                                          stats.back().columns[0].total_domain}));
				}
			}
			for (idx_t class_index = 0; class_index < 2; class_index++) {
				auto column = reverse_classes ? 1 - class_index : class_index;
				JoinEqualityClass group;
				group.index = class_index;
				vector<idx_t> targets = column == 0 ? vector<idx_t> {1, 2} : vector<idx_t> {2};
				if (reverse_edges) {
					std::reverse(targets.begin(), targets.end());
				}
				for (auto target : targets) {
					auto left = ColumnBinding(TableIndex(0), ProjectionIndex(column));
					auto right = ColumnBinding(TableIndex(target), ProjectionIndex(column));
					auto &lhs = sets.GetJoinRelation(RelationIndex(0));
					auto &rhs = sets.GetJoinRelation(RelationIndex(target));
					auto filter =
					    make_uniq<FilterInfo>(CreateJoinComparison(left, right, ExpressionType::COMPARE_EQUAL),
					                          sets.Union(lhs, rhs), filters.size());
					filter->SetLeftSet(lhs);
					filter->SetRightSet(rhs);
					filter->left_binding = left;
					filter->right_binding = right;
					auto &predicate = model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, left, right);
					predicate.SetEqualityClassIndex(optional_idx(class_index));
					group.columns.insert(left);
					group.columns.insert(right);
					group.relations.insert(RelationIndex(0));
					group.relations.insert(RelationIndex(target));
					group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(target), left, right);
					filters.push_back(std::move(filter));
				}
				model.AddEqualityClass(std::move(group));
			}
			CardinalityEstimator estimator(sets, model);
			estimator.Initialize(stats, {}, sources);
			auto &base = sets.Union(sets.GetJoinRelation(RelationIndex(0)), sets.GetJoinRelation(RelationIndex(1)));
			auto &all = sets.Union(base, sets.GetJoinRelation(RelationIndex(2)));
			REQUIRE(estimator.EstimateCardinality(base).GetIndex() == 5000);
			// Use domains 200 and 100 before the redundant local domain of 10.
			REQUIRE(estimator.EstimateCardinality(all).GetIndex() == 500);
		}
	}
}

TEST_CASE("Join cardinality divides large products before converting to double",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	const idx_t count = 20;
	const idx_t population = idx_t(1) << 60;
	for (idx_t rows : {population, population >> 4}) {
		for (bool equality : {false, true}) {
			CAPTURE(rows, equality);
			JoinRelationSetManager sets;
			JoinPredicateModel model;
			vector<unique_ptr<FilterInfo>> filters;
			vector<RelationStats> stats;
			JoinEqualityClass group;
			group.index = 0;
			auto root = &sets.GetJoinRelation(RelationIndex(0));
			for (idx_t i = 0; i < count; i++) {
				auto binding = ColumnBinding(TableIndex(i), ProjectionIndex(0));
				auto &relation = sets.GetJoinRelation(RelationIndex(i));
				stats.push_back(CreateStats({binding}, {rows}, rows));
				stats.back().columns[0].total_domain = DistinctCount(population, DistinctCountSource::EXACT);
				group.columns.insert(binding);
				group.relations.insert(RelationIndex(i));
				if (i > 0) {
					auto previous = ColumnBinding(TableIndex(i - 1), ProjectionIndex(0));
					auto &left = sets.GetJoinRelation(RelationIndex(i - 1));
					if (equality) {
						auto filter = make_uniq<FilterInfo>(
						    CreateJoinComparison(previous, binding, ExpressionType::COMPARE_EQUAL),
						    sets.Union(left, relation), i - 1);
						filter->SetLeftSet(left);
						filter->SetRightSet(relation);
						filter->left_binding = previous;
						filter->right_binding = binding;
						auto &predicate =
						    model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, previous, binding);
						predicate.SetEqualityClassIndex(optional_idx(0));
						group.edges.emplace_back(predicate, RelationIndex(i - 1), RelationIndex(i), previous, binding);
						filters.push_back(std::move(filter));
					}
					root = &sets.Union(*root, relation);
				}
			}
			if (equality) {
				model.AddEqualityClass(std::move(group));
			}
			CardinalityEstimator estimator(sets, model);

			estimator.Initialize(stats);
			if (equality) {
				// Neither product fits in a double. The quotients are 2^60 and 2^-20.
				auto expected = rows == population ? double(population) : std::ldexp(1.0, -20);
				REQUIRE(estimator.EstimateCardinality(*root).GetDouble() == Approx(expected));
				REQUIRE(estimator.EstimateCardinality(*root).GetIndex() == MaxValue<idx_t>(1, idx_t(expected)));
			} else {
				REQUIRE(std::isinf(estimator.EstimateCardinality(*root).GetDouble()));
				REQUIRE(estimator.EstimateCardinality(*root).GetIndex() == NumericLimits<idx_t>::Maximum());
			}
		}
	}
}

TEST_CASE("Logical cardinalities recover across cached overflowing and underflowing subsets",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	const idx_t population = idx_t(1) << 60;
	for (bool overflow : {false, true}) {
		CAPTURE(overflow);
		const idx_t count = overflow ? 21 : 22;
		JoinRelationSetManager sets;
		JoinPredicateModel model;
		vector<RelationStats> stats;
		vector<unique_ptr<FilterInfo>> filters;
		for (idx_t i = 0; i < count; i++) {
			const idx_t columns = overflow && i == 20 ? 20 : 2;
			const idx_t rows = overflow || i >= 20 ? population : 1;
			vector<ColumnBinding> bindings;
			vector<idx_t> domains;
			for (idx_t column = 0; column < columns; column++) {
				bindings.emplace_back(TableIndex(i), ProjectionIndex(column));
				domains.push_back(rows);
			}
			stats.push_back(CreateStats(bindings, domains, rows));
			for (auto &column : stats.back().columns) {
				column.total_domain = DistinctCount(population, DistinctCountSource::EXACT);
			}
		}
		for (idx_t i = 0; i < (overflow ? 20 : 19); i++) {
			auto right_index = overflow ? 20 : i + 1;
			auto a = ColumnBinding(TableIndex(i), ProjectionIndex(1));
			auto b = ColumnBinding(TableIndex(right_index), ProjectionIndex(overflow ? i : 0));
			auto &left = sets.GetJoinRelation(RelationIndex(i));
			auto &right = sets.GetJoinRelation(RelationIndex(right_index));
			auto filter = make_uniq<FilterInfo>(CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL),
			                                    sets.Union(left, right), i);
			filter->SetLeftSet(left);
			filter->SetRightSet(right);
			filter->left_binding = a;
			filter->right_binding = b;
			auto &predicate = model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, a, b);
			predicate.SetEqualityClassIndex(optional_idx(i));
			JoinEqualityClass group;
			group.index = i;
			group.columns = {a, b};
			group.relations = {RelationIndex(i), RelationIndex(right_index)};
			group.edges.emplace_back(predicate, RelationIndex(i), RelationIndex(right_index), a, b);
			model.AddEqualityClass(std::move(group));
			filters.push_back(std::move(filter));
		}
		CardinalityEstimator estimator(sets, model);

		estimator.Initialize(stats);
		reference<JoinRelationSet> prefix = sets.GetJoinRelation(RelationIndex(0));
		CardinalityValue rows(stats[0].cardinality);
		for (idx_t i = 1; i < count; i++) {
			auto &right = sets.GetJoinRelation(RelationIndex(i));
			prefix = sets.Union(prefix, right);
			rows = estimator.EstimateCardinality(prefix);
			if (i == 19) {
				// The stored candidate is 2^1200 or 2^-1140; both must survive conversion boundaries.
				REQUIRE_FALSE(rows.IsZero());
				if (overflow) {
					REQUIRE(std::isinf(rows.GetDouble()));
				} else {
					REQUIRE(rows.GetDouble() == 0);
				}
			}
		}
		auto expected = overflow ? double(population) : std::ldexp(1.0, -1020);
		REQUIRE(rows.GetDouble() == expected);
	}
}

TEST_CASE("Tiny positive cardinalities remain distinct from zero",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	CardinalityValue factor(std::ldexp(1.0, -600));
	auto tiny = factor * factor;
	REQUIRE(tiny.GetDouble() == 0);
	REQUIRE_FALSE(tiny.IsZero());
	REQUIRE(tiny.GetIndex() == 1);
	REQUIRE(CardinalityValue(0).GetIndex() == 0);
	REQUIRE(CardinalityValue(0.5).GetIndex() == 1);
	REQUIRE(CardinalityValue(2.5).GetIndex() == 2);
	REQUIRE(tiny > CardinalityValue(0));
	REQUIRE(tiny < factor);
	for (auto type : {JoinType::SEMI, JoinType::ANTI}) {
		auto positive = RelationStatisticsHelper::EstimateSemiAntiJoinFallbackCardinality(CardinalityValue(102),
		                                                                                  tiny.IsZero() ? 0 : 1, type);
		REQUIRE(positive.GetDouble() == Approx(20.4));
		auto empty = RelationStatisticsHelper::EstimateSemiAntiJoinFallbackCardinality(CardinalityValue(102), 0, type);
		REQUIRE(empty.GetDouble() == (type == JoinType::SEMI ? 0 : 102));
	}
}

TEST_CASE("Scaled cardinality sums and complements do not materialize intermediate doubles",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	CardinalityValue large(std::ldexp(1.0, 600));
	CardinalityValue small(std::ldexp(1.0, -600));
	auto huge = large * large;
	auto tiny = small * small;
	REQUIRE(std::isinf(huge.GetDouble()));
	REQUIRE(tiny.GetDouble() == 0);
	REQUIRE((huge + huge) / huge == 2);
	REQUIRE((tiny + tiny) / tiny == 2);
	REQUIRE((huge * 3 - huge) / huge == 2);
	REQUIRE((tiny * 3 - tiny) / tiny == 2);
	REQUIRE((tiny - huge).IsZero());
	REQUIRE((huge - huge).IsZero());
	REQUIRE((CardinalityValue(0.75) - CardinalityValue(0.5)).GetDouble() == 0.25);
	auto adjacent = CardinalityValue(std::nextafter(1.0, 2.0)) - CardinalityValue(1);
	REQUIRE(adjacent.GetDouble() == std::numeric_limits<double>::epsilon());
}

TEST_CASE("Row estimate floors never become emptiness proofs",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	RelationStats stats;
	stats.cardinality = 0;
	REQUIRE_FALSE(stats.IsEmpty());
	REQUIRE(stats.GetCardinalityForCost() == 1);
	REQUIRE(stats.GetCardinalityForPlan() == 1);
	stats.CapCurrentDomainsToCardinality();
	REQUIRE(stats.cardinality.IsZero());
	REQUIRE_FALSE(stats.IsEmpty());
	stats.cardinality = 0.25;
	REQUIRE(stats.GetCardinality() == 0.25);
	REQUIRE(stats.GetCardinalityForCost() == 1);
	REQUIRE(stats.GetCardinalityForPlan() == 1);
	stats.max_cardinality = optional_idx(0);
	REQUIRE(stats.IsEmpty());
	REQUIRE(stats.GetCardinalityForCost().IsZero());
	REQUIRE(stats.GetCardinalityForPlan() == 0);

	stats.max_cardinality = optional_idx(NumericLimits<idx_t>::Maximum() - 1);
	stats.cardinality = CardinalityValue(1e100);
	REQUIRE(stats.GetCardinalityForPlan() == NumericLimits<idx_t>::Maximum() - 1);
	REQUIRE_FALSE(RelationStatisticsHelper::MultiplyCardinalityBounds(optional_idx(NumericLimits<idx_t>::Maximum()),
	                                                                  optional_idx(2))
	                  .IsValid());
	REQUIRE(RelationStatisticsHelper::MultiplyCardinalityBounds(optional_idx(), optional_idx(0)).GetIndex() == 0);
}

TEST_CASE("Only uniquely owned column collections establish closed cardinality proofs",
          "[optimizer][relation_statistics][column_data_ownership]") {
	DuckDB db;
	Connection connection(db);
	auto &context = *connection.context;
	vector<LogicalType> types {LogicalType::INTEGER};
	auto left = CreateStats({ColumnBinding(TableIndex(10), ProjectionIndex(0))}, {10}, 100);
	auto append = [](ColumnDataCollection &collection) {
		DataChunk chunk;
		collection.InitializeScanChunk(chunk);
		chunk.SetChildCardinality(2);
		chunk.data[0].SetValue(0, Value::INTEGER(42));
		chunk.data[0].SetValue(1, Value::INTEGER(42));
		chunk.CheckCardinality(2);
		collection.Append(chunk);
	};
	auto semi_bound = [&](const RelationStats &right) {
		return RelationStatisticsHelper::JoinCardinalityBound(JoinType::SEMI, left.max_cardinality,
		                                                      right.max_cardinality);
	};

	for (bool empty : {true, false}) {
		CAPTURE(empty);
		auto collection = make_uniq<ColumnDataCollection>(context, types);
		if (!empty) {
			append(*collection);
		}
		LogicalColumnDataGet get(TableIndex(20), types, std::move(collection));
		auto stats = RelationStatisticsHelper::ExtractColumnDataGetStats(get, context);
		stats.Verify(get.GetColumnBindings());
		idx_t rows = empty ? 0 : 2;
		REQUIRE(stats.cardinality == rows);
		REQUIRE(stats.max_cardinality.IsValid());
		REQUIRE(stats.max_cardinality.GetIndex() == rows);
		REQUIRE(stats.IsEmpty() == empty);
		REQUIRE(stats.GetCardinalityForCost() == rows);
		REQUIRE(stats.columns[0].total_domain.source == DistinctCountSource::CARDINALITY);
		REQUIRE(stats.columns[0].total_domain.distinct_count == rows);
		auto bound = stats.columns[0].semi_anti_join_domain_evidence.GetDomainBound();
		REQUIRE(bound.IsValid());
		REQUIRE(bound.GetIndex() == rows);
		REQUIRE(semi_bound(stats).GetIndex() == (empty ? 0 : 100));
	}

	for (bool shared : {false, true}) {
		CAPTURE(shared);
		auto collection = make_shared_ptr<ColumnDataCollection>(context, types);
		auto collection_reference = shared ? optionally_owned_ptr<ColumnDataCollection>(collection)
		                                   : optionally_owned_ptr<ColumnDataCollection>(*collection);
		LogicalColumnDataGet get(TableIndex(20), types, std::move(collection_reference));
		REQUIRE(get.collection.is_owned() == shared);
		REQUIRE_FALSE(get.collection.get_owned_unique());
		REQUIRE(get.collection->Count() == 0);
		auto empty_stats = RelationStatisticsHelper::ExtractColumnDataGetStats(get, context);
		empty_stats.Verify(get.GetColumnBindings());
		REQUIRE(empty_stats.cardinality == 0);
		REQUIRE_FALSE(empty_stats.IsEmpty());
		REQUIRE(empty_stats.GetCardinalityForCost() == 1);
		REQUIRE(empty_stats.GetCardinalityForPlan() == 1);
		REQUIRE(empty_stats.columns[0].total_domain.distinct_count.IsZero());
		REQUIRE(empty_stats.columns[0].current_domain.distinct_count.IsZero());
		REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallbackCardinality(
		            left.cardinality, empty_stats.cardinality, JoinType::SEMI) == 0);
		REQUIRE_FALSE(empty_stats.max_cardinality.IsValid());
		REQUIRE_FALSE(empty_stats.columns[0].semi_anti_join_domain_evidence.GetDomainBound().IsValid());
		REQUIRE_FALSE(empty_stats.columns[0].GetSupportedSemiAntiDomainSize());
		REQUIRE(semi_bound(empty_stats).GetIndex() == 100);

		// Unary extraction consumes the raw snapshot; display floors are not row observations.
		LogicalLimit limit(BoundLimitNode::ConstantPercentage(50), BoundLimitNode());
		limit.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(20), types, *collection));
		auto limited = RelationStatisticsHelper::ExtractLimitStats(limit, empty_stats);
		REQUIRE(limited);
		REQUIRE(limited->cardinality == 0);
		REQUIRE_FALSE(limited->IsEmpty());
		REQUIRE_FALSE(limited->max_cardinality.IsValid());

		// Another owner can populate this collection after its planning snapshot was taken.
		append(*collection);
		REQUIRE(get.collection->Count() == 2);
		REQUIRE(empty_stats.cardinality == 0);
		REQUIRE_FALSE(empty_stats.IsEmpty());
		REQUIRE(semi_bound(empty_stats).GetIndex() == 100);
		auto populated_stats = RelationStatisticsHelper::ExtractColumnDataGetStats(get, context);
		populated_stats.Verify(get.GetColumnBindings());
		REQUIRE(populated_stats.cardinality == 2);
		REQUIRE(populated_stats.GetCardinalityForCost() == 2);
		REQUIRE_FALSE(populated_stats.IsEmpty());
		REQUIRE_FALSE(populated_stats.max_cardinality.IsValid());
		REQUIRE_FALSE(populated_stats.columns[0].semi_anti_join_domain_evidence.GetDomainBound().IsValid());
		REQUIRE_FALSE(populated_stats.columns[0].GetSupportedSemiAntiDomainSize());
		REQUIRE(populated_stats.columns[0].total_domain.source == DistinctCountSource::CARDINALITY);
		REQUIRE(populated_stats.columns[0].total_domain.distinct_count == 2);
		REQUIRE(semi_bound(populated_stats).GetIndex() == 100);
	}
}

TEST_CASE("SEMI and ANTI estimator arithmetic preserves fractional inputs and outputs",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	for (bool fractional_input : {false, true}) {
		for (auto join_type : {JoinType::SEMI, JoinType::ANTI}) {
			for (idx_t rhs_rows : {idx_t(0), idx_t(1)}) {
				CAPTURE(fractional_input, join_type, rhs_rows);
				JoinRelationSetManager sets;
				auto &left = sets.GetJoinRelation(RelationIndex(0));
				auto &second = sets.GetJoinRelation(RelationIndex(1));
				auto &rhs = sets.GetJoinRelation(RelationIndex(2));
				auto &pair = sets.Union(left, second);
				auto &preserved = fractional_input ? pair : left;
				auto &root = sets.Union(preserved, rhs);
				auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
				auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
				auto c = ColumnBinding(TableIndex(2), ProjectionIndex(0));
				FilterInfo equality(CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL), pair, 0);
				equality.SetLeftSet(left);
				equality.SetRightSet(second);
				equality.left_binding = a;
				equality.right_binding = b;
				FilterInfo semi(CreateJoinComparison(a, c, ExpressionType::COMPARE_EQUAL), root, 1, join_type);
				semi.SetLeftSet(preserved);
				semi.SetRightSet(rhs);
				semi.semantic_left_set = preserved;
				semi.semantic_right_set = rhs;
				JoinPredicateModel model;
				if (fractional_input) {
					auto &predicate = model.RegisterPredicate(equality, JoinPredicateClass::INNER_EQUALITY, a, b);
					predicate.SetEqualityClassIndex(optional_idx(0));
					JoinEqualityClass group;
					group.index = 0;
					group.columns = {a, b};
					group.relations = {RelationIndex(0), RelationIndex(1)};
					group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(1), a, b);
					model.AddEqualityClass(std::move(group));
				}
				model.RegisterPredicate(semi, JoinPredicateClass::SEMI_ANTI_JOIN, a, c);
				auto left_stats =
				    CreateStats({a}, {fractional_input ? idx_t(2) : idx_t(102)}, fractional_input ? 2 : 102);
				auto second_stats = CreateStats({b}, {1}, 1);
				if (fractional_input) {
					left_stats.columns[0].total_domain = DistinctCount(4, DistinctCountSource::EXACT);
					second_stats.columns[0].total_domain = DistinctCount(4, DistinctCountSource::EXACT);
				}
				auto rhs_stats = CreateStats({c}, {rhs_rows}, rhs_rows);
				CardinalityEstimator estimator(sets, model);

				vector<unique_ptr<JoinOrderOperator>> operators;
				auto type = join_type == JoinType::SEMI ? JoinOrderOperatorType::SEMI : JoinOrderOperatorType::ANTI;
				AddOperator(operators, sets, preserved, rhs, type);
				estimator.Initialize({left_stats, second_stats, rhs_stats}, operators);
				double input = fractional_input ? 0.5 : 102;
				auto expected = rhs_rows ? input * 0.2 : (join_type == JoinType::ANTI ? input : 0);
				auto estimate = [&]() {
					return estimator.EstimateCardinality(root);
				};
				REQUIRE(estimate().GetDouble() == Approx(expected));
				REQUIRE(estimator.EstimateCardinality(preserved).GetDouble() == Approx(input));
				estimator.EstimateCardinality(root);
				REQUIRE(estimate().GetDouble() == Approx(expected));
			}
		}
	}
}

TEST_CASE("Large LEFT and composite joins retain their cardinality bounds",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	const idx_t count = 20;
	const idx_t population = idx_t(1) << 60;
	for (auto configuration : {make_pair(JoinType::LEFT, population / 2), make_pair(JoinType::LEFT, population * 2),
	                           make_pair(JoinType::INNER, population / 2)}) {
		auto join_type = configuration.first;
		auto final_domain = configuration.second;
		CAPTURE(join_type, final_domain);
		JoinRelationSetManager sets;
		JoinPredicateModel model;
		vector<unique_ptr<FilterInfo>> filters;
		vector<RelationStats> stats;
		JoinEqualityClass chain;
		chain.index = 0;
		auto root = &sets.GetJoinRelation(RelationIndex(0));
		for (idx_t i = 0; i < count; i++) {
			auto binding = ColumnBinding(TableIndex(i), ProjectionIndex(0));
			auto first_key = ColumnBinding(TableIndex(i), ProjectionIndex(1));
			auto second_key = ColumnBinding(TableIndex(i), ProjectionIndex(2));
			stats.push_back(CreateStats(
			    {binding, first_key, second_key},
			    {population, MinValue(population, final_domain), MinValue(population, final_domain)}, population));
			stats.back().columns[0].total_domain = DistinctCount(population * 4, DistinctCountSource::EXACT);
			stats.back().columns[1].total_domain = DistinctCount(final_domain, DistinctCountSource::EXACT);
			stats.back().columns[2].total_domain = DistinctCount(final_domain, DistinctCountSource::EXACT);
			if (i == count - 1) {
				break;
			}
			chain.columns.insert(binding);
			chain.relations.insert(RelationIndex(i));
			if (i == 0) {
				continue;
			}
			auto previous = ColumnBinding(TableIndex(i - 1), ProjectionIndex(0));
			auto &left = sets.GetJoinRelation(RelationIndex(i - 1));
			auto &right = sets.GetJoinRelation(RelationIndex(i));
			auto filter = make_uniq<FilterInfo>(CreateJoinComparison(previous, binding, ExpressionType::COMPARE_EQUAL),
			                                    sets.Union(left, right), filters.size());
			filter->SetLeftSet(left);
			filter->SetRightSet(right);
			filter->left_binding = previous;
			filter->right_binding = binding;
			auto &predicate = model.RegisterPredicate(*filter, JoinPredicateClass::INNER_EQUALITY, previous, binding);
			predicate.SetEqualityClassIndex(optional_idx(0));
			chain.edges.emplace_back(predicate, RelationIndex(i - 1), RelationIndex(i), previous, binding);
			filters.push_back(std::move(filter));
			root = &sets.Union(*root, right);
		}
		model.AddEqualityClass(std::move(chain));
		auto &last_left = sets.GetJoinRelation(RelationIndex(count - 2));
		auto &right = sets.GetJoinRelation(RelationIndex(count - 1));
		auto &pair = sets.Union(last_left, right);
		auto &left = last_left;
		for (idx_t key = 1; key <= (join_type == JoinType::LEFT ? 1 : 2); key++) {
			auto a = ColumnBinding(TableIndex(count - 2), ProjectionIndex(key));
			auto b = ColumnBinding(TableIndex(count - 1), ProjectionIndex(key));
			auto filter = make_uniq<FilterInfo>(CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL),
			                                    sets.Union(left, right), filters.size(), join_type);
			filter->SetLeftSet(left);
			filter->SetRightSet(right);
			filter->left_binding = a;
			filter->right_binding = b;
			filter->source_operator_index = optional_idx(0);
			auto &predicate = model.RegisterPredicate(
			    *filter,
			    join_type == JoinType::LEFT ? JoinPredicateClass::LEFT_JOIN : JoinPredicateClass::INNER_EQUALITY, a, b);
			if (join_type == JoinType::INNER) {
				predicate.SetEqualityClassIndex(optional_idx(key));
				JoinEqualityClass group;
				group.index = key;
				group.columns = {a, b};
				group.relations = {RelationIndex(count - 2), RelationIndex(count - 1)};
				group.edges.emplace_back(predicate, RelationIndex(count - 2), RelationIndex(count - 1), a, b);
				model.AddEqualityClass(std::move(group));
				model.AddDirectEqualityPairClass(pair, key, a, b);
			}
			filters.push_back(std::move(filter));
		}
		auto &preserved_set = *root;
		root = &sets.Union(*root, right);
		CardinalityEstimator estimator(sets, model);

		vector<unique_ptr<JoinOrderOperator>> operators;
		auto type = join_type == JoinType::LEFT ? JoinOrderOperatorType::LEFT : JoinOrderOperatorType::INNER;
		AddOperator(operators, sets, preserved_set, right, type);
		estimator.Initialize(stats, operators);
		// The chain has 2^24 rows despite its overflowing numerator.
		// LEFT doubles/preserves this input; the composite cap preserves it.
		auto preserved_cardinality = population >> (2 * (count - 2));
		auto expected = preserved_cardinality * (join_type == JoinType::LEFT && final_domain < population ? 2 : 1);
		auto rows = estimator.EstimateCardinality(*root);
		REQUIRE(rows.GetDouble() == Approx(double(expected)));
		REQUIRE(LossyNumericCast<idx_t>(rows.GetDouble()) == expected);
	}
}

TEST_CASE("Deferred OR selectivity is applied once through nested SEMI joins", "[optimizer][relation_statistics]") {
	JoinRelationSetManager set_manager;
	auto &first = set_manager.GetJoinRelation(RelationIndex(0));
	auto &second = set_manager.GetJoinRelation(RelationIndex(1));
	auto &third = set_manager.GetJoinRelation(RelationIndex(2));
	auto &fourth = set_manager.GetJoinRelation(RelationIndex(3));
	auto &preserved = set_manager.Union(first, second);
	auto &inner = set_manager.Union(preserved, third);
	auto &root = set_manager.Union(inner, fourth);
	auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
	auto c = ColumnBinding(TableIndex(2), ProjectionIndex(0));
	auto d = ColumnBinding(TableIndex(3), ProjectionIndex(0));
	FilterInfo or_filter(make_uniq<BoundConjunctionExpression>(
	                         ExpressionType::CONJUNCTION_OR, CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL),
	                         CreateJoinComparison(a, b, ExpressionType::COMPARE_GREATERTHAN)),
	                     preserved, 0);
	FilterInfo inner_filter(CreateJoinComparison(a, c, ExpressionType::COMPARE_EQUAL), inner, 1, JoinType::SEMI);
	inner_filter.SetLeftSet(preserved);
	inner_filter.SetRightSet(third);
	inner_filter.semantic_left_set = preserved;
	inner_filter.semantic_right_set = third;
	FilterInfo outer_filter(CreateJoinComparison(a, d, ExpressionType::COMPARE_EQUAL), root, 2, JoinType::SEMI);
	outer_filter.SetLeftSet(inner);
	outer_filter.SetRightSet(fourth);
	outer_filter.semantic_left_set = inner;
	outer_filter.semantic_right_set = fourth;
	JoinPredicateModel model;
	model.RegisterPredicate(or_filter, JoinPredicateClass::OTHER, a, b);
	model.RegisterPredicate(inner_filter, JoinPredicateClass::SEMI_ANTI_JOIN, a, c);
	model.RegisterPredicate(outer_filter, JoinPredicateClass::SEMI_ANTI_JOIN, a, d);
	CardinalityEstimator estimator(set_manager, model);

	auto first_stats = CreateStats({a}, {1000}, 1000);
	auto second_stats = CreateStats({b}, {1}, 1);
	auto third_stats = CreateStats({c}, {100}, 100);
	auto fourth_stats = CreateStats({d}, {100}, 100);
	vector<unique_ptr<JoinOrderOperator>> operators;
	AddOperator(operators, set_manager, preserved, third, JoinOrderOperatorType::SEMI);
	AddOperator(operators, set_manager, inner, fourth, JoinOrderOperatorType::SEMI);
	estimator.Initialize({first_stats, second_stats, third_stats, fourth_stats}, operators);
	auto estimate = [&]() {
		REQUIRE(estimator.EstimateCardinality(preserved).GetDouble() == 200);
		REQUIRE(estimator.EstimateCardinality(inner).GetDouble() == 40);
		return estimator.EstimateCardinality(root);
	};
	REQUIRE(estimate().GetDouble() == 8);
	estimator.EstimateCardinality(root);
	estimator.EstimateCardinality(inner);
	REQUIRE(estimate().GetDouble() == 8);
}

TEST_CASE("SEMI and ANTI residual OR conditions use one fallback", "[optimizer][relation_statistics]") {
	for (auto join_type : {JoinType::SEMI, JoinType::ANTI}) {
		JoinRelationSetManager set_manager;
		auto &left = set_manager.GetJoinRelation(RelationIndex(0));
		auto &right = set_manager.GetJoinRelation(RelationIndex(1));
		auto &root = set_manager.Union(left, right);
		auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
		auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
		FilterInfo filter(make_uniq<BoundConjunctionExpression>(
		                      ExpressionType::CONJUNCTION_OR, CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL),
		                      CreateJoinComparison(a, b, ExpressionType::COMPARE_GREATERTHAN)),
		                  root, 0, join_type);
		filter.SetLeftSet(left);
		filter.SetRightSet(right);
		filter.semantic_left_set = left;
		filter.semantic_right_set = right;
		JoinPredicateModel model;
		model.RegisterPredicate(filter, JoinPredicateClass::SEMI_ANTI_JOIN, a, b);
		CardinalityEstimator estimator(set_manager, model);

		auto left_stats = CreateStats({a}, {1000}, 1000);
		auto right_stats = CreateStats({b}, {100}, 100);
		vector<unique_ptr<JoinOrderOperator>> operators;
		auto type = join_type == JoinType::SEMI ? JoinOrderOperatorType::SEMI : JoinOrderOperatorType::ANTI;
		AddOperator(operators, set_manager, left, right, type);
		estimator.Initialize({left_stats, right_stats}, operators);
		auto estimate = [&]() {
			return estimator.EstimateCardinality(root);
		};
		REQUIRE(estimate().GetDouble() == 200);
		estimator.EstimateCardinality(root);
		REQUIRE(estimate().GetDouble() == 200);
	}
}

TEST_CASE("Completed operators use their projected logical input scopes", "[optimizer][relation_statistics]") {
	for (auto type : {JoinOrderOperatorType::LEFT, JoinOrderOperatorType::SEMI, JoinOrderOperatorType::ANTI}) {
		for (idx_t extra_rows : {idx_t(0), idx_t(10)}) {
			JoinRelationSetManager sets;
			auto &a = sets.GetJoinRelation(RelationIndex(0));
			auto &b = sets.GetJoinRelation(RelationIndex(1));
			auto &c = sets.GetJoinRelation(RelationIndex(2));
			auto &ab = sets.Union(a, b);
			auto &ac = sets.Union(a, c);
			auto &root = sets.Union(ab, c);
			JoinPredicateModel model;
			vector<unique_ptr<JoinOrderOperator>> operators;
			AddOperator(operators, sets, a, c, JoinOrderOperatorType::LEFT);
			auto &outer = AddOperator(operators, sets, ac, b, type);
			outer.total_set = ab;
			outer.left_total_set = a;
			vector<RelationStats> stats;
			for (idx_t i = 0; i < 3; i++) {
				auto rows = i == 0 ? 100 : i == 1 ? 20 : extra_rows;
				stats.push_back(CreateStats({ColumnBinding(TableIndex(i), ProjectionIndex(0))}, {rows}, rows));
			}
			for (bool root_first : {false, true}) {
				CardinalityEstimator estimator(sets, model);
				estimator.Initialize(stats, operators);
				auto expected_ab = type == JoinOrderOperatorType::LEFT ? 2000.0 : 20.0;
				auto expected_root = expected_ab * MaxValue(idx_t(1), extra_rows);
				if (root_first) {
					REQUIRE(estimator.EstimateCardinality(root).GetDouble() == expected_root);
				}
				REQUIRE(estimator.EstimateCardinality(ab).GetDouble() == expected_ab);
				REQUIRE(estimator.EstimateCardinality(root).GetDouble() == expected_root);
			}
		}
	}
}

TEST_CASE("SEMI boundaries use the same estimate before or after an INNER candidate",
          "[optimizer][relation_statistics]") {
	JoinRelationSetManager sets;
	auto &a = sets.GetJoinRelation(RelationIndex(0));
	auto &b = sets.GetJoinRelation(RelationIndex(1));
	auto &c = sets.GetJoinRelation(RelationIndex(2));
	auto &ab = sets.Union(a, b);
	auto &ac = sets.Union(a, c);
	auto &root = sets.Union(ab, c);
	auto x = ColumnBinding(TableIndex(0), ProjectionIndex(0));
	auto y = ColumnBinding(TableIndex(1), ProjectionIndex(0));
	auto z = ColumnBinding(TableIndex(2), ProjectionIndex(0));
	FilterInfo filter(CreateJoinComparison(x, z, ExpressionType::COMPARE_EQUAL), ac, 0);
	filter.SetLeftSet(a);
	filter.SetRightSet(c);
	filter.left_binding = x;
	filter.right_binding = z;
	JoinPredicateModel model;
	auto &predicate = model.RegisterPredicate(filter, JoinPredicateClass::INNER_EQUALITY, x, z);
	predicate.SetEqualityClassIndex(optional_idx(0));
	JoinEqualityClass group;
	group.index = 0;
	group.columns = {x, z};
	group.relations = {RelationIndex(0), RelationIndex(2)};
	group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(2), x, z);
	model.AddEqualityClass(std::move(group));
	auto left_stats = CreateStats({x}, {1}, 1);
	left_stats.columns[0].total_domain = DistinctCount(1250, DistinctCountSource::EXACT);
	vector<RelationStats> stats {left_stats, CreateStats({y}, {10}, 10), CreateStats({z}, {1250}, 10000)};
	vector<unique_ptr<JoinOrderOperator>> operators;
	AddOperator(operators, sets, a, b, JoinOrderOperatorType::SEMI);
	for (bool root_first : {false, true}) {
		CardinalityEstimator estimator(sets, model);
		estimator.Initialize(stats, operators);
		if (root_first) {
			REQUIRE(estimator.EstimateCardinality(root).GetDouble() == Approx(1.6));
		}
		REQUIRE(estimator.EstimateCardinality(ab).GetDouble() == 0.2);
		REQUIRE(estimator.EstimateCardinality(ac).GetDouble() == 8);
		// The parent consumes 0.2 retained rows; displaying AB as one row must not
		// inflate the parent to eight rows.
		auto estimate = estimator.EstimateCardinality(root);
		REQUIRE(estimate.GetDouble() == Approx(1.6));
	}
}

TEST_CASE("LEFT ON residuals precede preservation and post-filters follow it", "[optimizer][relation_statistics]") {
	for (idx_t right_rows : {idx_t(1), idx_t(10000)}) {
		for (bool post_filter : {false, true}) {
			JoinRelationSetManager sets;
			auto &left = sets.GetJoinRelation(RelationIndex(0));
			auto &right = sets.GetJoinRelation(RelationIndex(1));
			auto &root = sets.Union(left, right);
			auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
			auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
			FilterInfo equality(CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL), root, 0, JoinType::LEFT);
			equality.SetLeftSet(left);
			equality.SetRightSet(right);
			equality.left_binding = a;
			equality.right_binding = b;
			equality.source_operator_index = optional_idx(0);
			FilterInfo residual(
			    make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR,
			                                          CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL),
			                                          CreateJoinComparison(a, b, ExpressionType::COMPARE_GREATERTHAN)),
			    root, 1);
			residual.must_remain_filter = true;
			JoinPredicateModel model;
			model.RegisterPredicate(equality, JoinPredicateClass::LEFT_JOIN, a, b);
			if (post_filter) {
				model.RegisterPredicate(residual, JoinPredicateClass::OTHER, a, b);
			}
			vector<unique_ptr<JoinOrderOperator>> operators;
			auto &op = AddOperator(operators, sets, left, right, JoinOrderOperatorType::LEFT);
			op.conditions.emplace_back(residual.filter->Copy());
			op.residual_predicates.push_back(op.conditions.back().GetJoinExpression());
			CardinalityEstimator estimator(sets, model);
			estimator.Initialize(
			    {CreateStats({a}, {100}, 100), CreateStats({b}, {MinValue(idx_t(100), right_rows)}, right_rows)},
			    operators);
			auto expected = right_rows == 1 ? 100.0 : 2000.0;
			if (post_filter) {
				expected *= 0.2;
			}
			REQUIRE(estimator.EstimateCardinality(root).GetDouble() == Approx(expected));
		}
	}
}

TEST_CASE("Owned INNER residuals wait for their complete operator", "[optimizer][relation_statistics]") {
	for (bool has_bindings : {false, true}) {
		JoinRelationSetManager sets;
		auto &a = sets.GetJoinRelation(RelationIndex(0));
		auto &b = sets.GetJoinRelation(RelationIndex(1));
		auto &d = sets.GetJoinRelation(RelationIndex(2));
		auto &ab = sets.Union(a, b);
		auto &ad = sets.Union(a, d);
		auto &root = sets.Union(ab, d);
		auto x = ColumnBinding(TableIndex(0), ProjectionIndex(0));
		auto y = ColumnBinding(TableIndex(1), ProjectionIndex(0));
		auto z = ColumnBinding(TableIndex(2), ProjectionIndex(0));
		unique_ptr<Expression> expression;
		if (has_bindings) {
			expression = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_OR, CreateJoinComparison(x, z, ExpressionType::COMPARE_EQUAL),
			    CreateJoinComparison(x, z, ExpressionType::COMPARE_GREATERTHAN));
		} else {
			expression = make_uniq<BoundParameterExpression>(Identifier("1"));
		}
		auto &referenced = has_bindings ? ad : sets.GetJoinRelation(unordered_set<RelationIndex>());
		FilterInfo residual(std::move(expression), referenced, 0);
		residual.source_operator_index = optional_idx(1);
		residual.from_residual_predicate = true;
		JoinPredicateModel model;
		model.RegisterPredicate(residual, JoinPredicateClass::OTHER, ColumnBinding(), ColumnBinding());
		vector<unique_ptr<JoinOrderOperator>> operators;
		AddOperator(operators, sets, a, b, JoinOrderOperatorType::LEFT);
		auto &op = AddOperator(operators, sets, ab, d, JoinOrderOperatorType::INNER);
		op.must_apply_as_operator = true;
		CardinalityEstimator estimator(sets, model);
		estimator.Initialize({CreateStats({x}, {100}, 100), CreateStats({y}, {1}, 1), CreateStats({z}, {10}, 10)},
		                     operators);
		REQUIRE(estimator.EstimateCardinality(ad).GetDouble() == 1000);
		REQUIRE(estimator.EstimateCardinality(ab).GetDouble() == 100);
		REQUIRE(estimator.EstimateCardinality(root).GetDouble() == 200);
	}
}

TEST_CASE("Projection and window preserve the statistics source", "[optimizer][relation_statistics]") {
	auto input = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto child_stats = CreateStats({input}, {10}, 100);
	child_stats.row_retention = 0.04;
	child_stats.columns[0].total_domain = DistinctCount(100, DistinctCountSource::EXACT);
	child_stats.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(10);
	auto projection = CreateProjectedDummy(TableIndex(10), TableIndex(20));
	auto projected =
	    RelationStatisticsHelper::ExtractProjectionStats(projection->Cast<LogicalProjection>(), child_stats);
	REQUIRE(projected);
	REQUIRE(projected->row_retention == child_stats.row_retention);
	REQUIRE(*projected->columns[0].GetSupportedSemiAntiDomainSize() == 10);
	LogicalWindow window(TableIndex(30), optional_idx());
	window.children.push_back(std::move(projection));
	auto windowed = RelationStatisticsHelper::ExtractWindowStats(window, *projected);
	REQUIRE(windowed);
	REQUIRE(windowed->row_retention == child_stats.row_retention);
	REQUIRE(*windowed->columns[0].GetSupportedSemiAntiDomainSize() == 10);
}

TEST_CASE("Grouping sets discard inherited SEMI domain evidence", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto input = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto child_stats = CreateStats({input}, {1}, 1);
	child_stats.columns[0].semi_anti_join_domain_evidence.MarkUnique();
	child_stats.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(1);
	LogicalAggregate aggregate(TableIndex(20), TableIndex(30), vector<unique_ptr<Expression>>());
	aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, input));
	aggregate.grouping_sets.emplace_back();
	aggregate.grouping_sets[0].insert(ProjectionIndex(0));
	auto ordinary = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(ordinary);
	REQUIRE(ordinary->columns[0].GetSupportedSemiAntiDomainSize());
	aggregate.grouping_sets.emplace_back();
	auto rollup = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(rollup);
	REQUIRE_FALSE(rollup->columns[0].GetSupportedSemiAntiDomainSize());
	aggregate.grouping_sets[1].insert(ProjectionIndex(0));
	auto duplicate_groups =
	    RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(duplicate_groups);
	REQUIRE_FALSE(duplicate_groups->columns[0].GetSupportedSemiAntiDomainSize());
}

//! Casts JSON nested 'depth' levels deep to VARIANT in a scan, so that no constant is folded on the way
static Value DeepVariantValue(Connection &connection, idx_t depth) {
	auto result = connection.Query("SELECT (repeat('[', i) || repeat(']', i))::JSON::VARIANT FROM range(" +
	                               to_string(depth) + ", " + to_string(depth + 1) + ") t(i)");
	REQUIRE_NO_FAIL(*result);
	return result->Collection().GetValue(0, 0);
}

TEST_CASE("Projection and aggregate statistics do not render a nested VARIANT constant",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	// rendering a VARIANT as SQL fails beyond 127 nested arrays, so naming a column after this value would fail
	auto deep = DeepVariantValue(connection, 1000);
	auto child_stats = CreateStats({}, {}, 3);
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(deep));
	LogicalProjection projection(TableIndex(20), std::move(expressions));
	auto projected = RelationStatisticsHelper::ExtractProjectionStats(projection, child_stats);
	REQUIRE(projected);
	REQUIRE(projected->columns.size() == 1);
	REQUIRE(projected->columns[0].total_domain.distinct_count == 1);

	FunctionBinder function_binder(*connection.context);
	vector<unique_ptr<Expression>> first_children;
	first_children.push_back(make_uniq<BoundConstantExpression>(deep));
	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(function_binder.BindAggregateFunction(FirstFunctionGetter::GetFunction(deep.type()),
	                                                           std::move(first_children), nullptr,
	                                                           AggregateType::NON_DISTINCT));
	LogicalAggregate aggregate(TableIndex(30), TableIndex(40), std::move(aggregates));
	aggregate.groups.push_back(make_uniq<BoundConstantExpression>(deep));
	auto aggregated = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(aggregated);
	REQUIRE(aggregated->columns.size() == 2);
}

#ifndef DEBUG
// debug builds render every plan expression in LogicalOperator::Verify, where this depth fails
TEST_CASE("Statements over a nested VARIANT constant plan without rendering it", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	string deep = "(repeat('[', 150) || repeat(']', 150))::JSON::VARIANT";
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE t (id INTEGER, doc VARIANT)"));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO t VALUES (1, NULL)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE s AS SELECT range AS x FROM range(3)"));
	REQUIRE_NO_FAIL(connection.Query("UPDATE t SET doc = " + deep + " WHERE id = 1"));
	auto result = connection.Query("SELECT variant_typeof(first(" + deep + ")) FROM s");
	REQUIRE(CHECK_COLUMN(result, 0, {"ARRAY(1)"}));
	result = connection.Query("SELECT count(*) FROM s GROUP BY [x, " + deep + "]");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1}));
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO t SELECT x + 2, " + deep + " FROM s"));
	// an INSERT of the constant itself never named it, and stores the same value
	REQUIRE_NO_FAIL(connection.Query("INSERT INTO t VALUES (5, " + deep + ")"));
	result = connection.Query(
	    "SELECT count(*), count(DISTINCT doc), count(*) FILTER (variant_typeof(doc) = 'ARRAY(1)') FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
}
#endif

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

TEST_CASE("Delim SEMI and ANTI joins use domain coverage", "[optimizer][relation_statistics]") {
	auto left_binding = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto right_binding = ColumnBinding(TableIndex(20), ProjectionIndex(0));
	auto left_stats = CreateStats({left_binding}, {1000}, 1000);
	auto right_stats = CreateStats({right_binding}, {100}, 100);
	right_stats.columns[0].total_domain = DistinctCount(1000, DistinctCountSource::EXACT);
	right_stats.row_retention = 0.1;
	right_stats.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(100);
	LogicalComparisonJoin delim_join(JoinType::SEMI, LogicalOperatorType::LOGICAL_DELIM_JOIN);
	delim_join.conditions.emplace_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left_binding),
	                                   make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right_binding),
	                                   ExpressionType::COMPARE_EQUAL);
	REQUIRE(
	    RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(delim_join, left_stats, right_stats).cardinality ==
	    100);
	delim_join.join_type = JoinType::ANTI;
	REQUIRE(
	    RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(delim_join, left_stats, right_stats).cardinality ==
	    900);
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

TEST_CASE("Materialized CTE extraction restores nested lookup scopes", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto outer_index = TableIndex(30);
	auto inner_index = TableIndex(40);
	auto definition = make_uniq<LogicalExpressionGet>(TableIndex(10), vector<LogicalType> {LogicalType::INTEGER},
	                                                  vector<vector<unique_ptr<Expression>>> {});
	definition->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(11)));
	for (int32_t value = 1; value <= 2; value++) {
		vector<unique_ptr<Expression>> row;
		row.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(value)));
		definition->expressions.push_back(std::move(row));
	}
	auto outer_ref = make_uniq<LogicalCTERef>(TableIndex(50), outer_index, vector<LogicalType> {LogicalType::INTEGER},
	                                          vector<Identifier> {Identifier("i")});
	auto inner_ref = make_uniq<LogicalCTERef>(TableIndex(60), inner_index, vector<LogicalType> {LogicalType::INTEGER},
	                                          vector<Identifier> {Identifier("i")});
	auto inner = make_uniq<LogicalMaterializedCTE>(Identifier("inner"), inner_index, 1, std::move(outer_ref),
	                                               std::move(inner_ref), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	LogicalMaterializedCTE outer(Identifier("outer"), outer_index, 1, std::move(definition), std::move(inner),
	                             CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	auto outside = CreateProjectedDummy(TableIndex(70), TableIndex(80));
	idx_t outside_lookups = 0;
	RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		REQUIRE((index == outer_index || index == inner_index));
		outside_lookups++;
		return *outside;
	});
	auto nested_stats = extractor.Extract(outer);
	REQUIRE(nested_stats);
	REQUIRE(nested_stats->cardinality == 2);
	REQUIRE(nested_stats->MatchesBindings(outer.GetColumnBindings()));
	REQUIRE(outside_lookups == 0);

	auto source_stats = make_shared_ptr<JoinOrderSourceStats>(*connection.context, outer);
	RelationStatsExtractor inherited_extractor(*connection.context, source_stats);
	auto inherited_stats = inherited_extractor.Extract(outer);
	REQUIRE(inherited_stats);
	REQUIRE(inherited_stats->cardinality == 2);
	REQUIRE(inherited_stats->MatchesBindings(outer.GetColumnBindings()));

	LogicalCTERef outside_outer(TableIndex(90), outer_index, {LogicalType::INTEGER}, {Identifier("i")});
	LogicalCTERef outside_inner(TableIndex(100), inner_index, {LogicalType::INTEGER}, {Identifier("i")});
	vector<reference<LogicalCTERef>> outside_references {outside_outer, outside_inner};
	for (auto &cte_ref : outside_references) {
		auto stats = extractor.Extract(cte_ref);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == 1);
		REQUIRE(stats->MatchesBindings(cte_ref.get().GetColumnBindings()));
	}
	REQUIRE(outside_lookups == 2);
}

TEST_CASE("SEMI fallback retains fractional cardinalities for enumeration", "[optimizer][relation_statistics]") {
	for (auto join_type : {JoinType::SEMI, JoinType::ANTI, JoinType::RIGHT_SEMI, JoinType::RIGHT_ANTI}) {
		auto estimate = RelationStatisticsHelper::EstimateSemiAntiJoinFallback(102, 1, join_type);
		REQUIRE(estimate.cardinality.GetDouble() == Approx(20.4));
		REQUIRE_FALSE(estimate.used_domain_evidence);
		REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(0, 1, join_type).cardinality == 0);
		REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(0.5, 1, join_type).cardinality.GetDouble() ==
		        Approx(0.1));
	}
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(102, 0, JoinType::SEMI).cardinality == 0);
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(102, 0, JoinType::ANTI).cardinality == 102);
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(102, 0, JoinType::RIGHT_SEMI).cardinality == 0);
	REQUIRE(RelationStatisticsHelper::EstimateSemiAntiJoinFallback(102, 0, JoinType::RIGHT_ANTI).cardinality == 102);
}

TEST_CASE("SEMI and ANTI fallback is independent of positive RHS cardinality", "[optimizer][relation_statistics]") {
	for (auto type : {JoinOrderOperatorType::SEMI, JoinOrderOperatorType::ANTI}) {
		for (idx_t left_rows : {idx_t(100003), idx_t(11997210)}) {
			double expected = 0;
			for (idx_t right_rows : {idx_t(1), idx_t(3), idx_t(17), idx_t(1000003), idx_t(59986052)}) {
				JoinRelationSetManager sets;
				auto &a = sets.GetJoinRelation(RelationIndex(0));
				auto &b = sets.GetJoinRelation(RelationIndex(1));
				auto &left = sets.Union(a, b);
				auto &right = sets.GetJoinRelation(RelationIndex(2));
				auto &root = sets.Union(left, right);
				auto x = ColumnBinding(TableIndex(0), ProjectionIndex(0));
				auto y = ColumnBinding(TableIndex(1), ProjectionIndex(0));
				auto z = ColumnBinding(TableIndex(2), ProjectionIndex(0));
				FilterInfo filter(CreateJoinComparison(x, y, ExpressionType::COMPARE_EQUAL), left, 0);
				filter.SetLeftSet(a);
				filter.SetRightSet(b);
				filter.left_binding = x;
				filter.right_binding = y;
				JoinPredicateModel model;
				auto &predicate = model.RegisterPredicate(filter, JoinPredicateClass::INNER_EQUALITY, x, y);
				predicate.SetEqualityClassIndex(optional_idx(0));
				JoinEqualityClass group;
				group.index = 0;
				group.columns = {x, y};
				group.relations = {RelationIndex(0), RelationIndex(1)};
				group.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(1), x, y);
				model.AddEqualityClass(std::move(group));
				vector<unique_ptr<JoinOrderOperator>> operators;
				AddOperator(operators, sets, left, right, type);
				CardinalityEstimator estimator(sets, model);
				estimator.Initialize(
				    {CreateStats({x}, {7}, left_rows), CreateStats({y}, {7}, 31), CreateStats({z}, {1}, right_rows)},
				    operators);
				auto rows = estimator.EstimateCardinality(root).GetDouble();
				if (right_rows == 1) {
					expected = rows;
				}
				// Even a one-ULP difference can bypass the enumerator's cost tie-breaker.
				REQUIRE(rows == expected);
			}
		}
	}
}

TEST_CASE("Shared key filters restrict domains only with supported input statistics",
          "[optimizer][relation_statistics]") {
	for (auto source : {DistinctCountSource::MIN_MAX, DistinctCountSource::HLL, DistinctCountSource::EXACT,
	                    DistinctCountSource::CARDINALITY}) {
		for (bool filter_both : {false, true}) {
			for (idx_t bound : {idx_t(1), idx_t(2)}) {
				JoinRelationSetManager sets;
				auto &left = sets.GetJoinRelation(RelationIndex(0));
				auto &right = sets.GetJoinRelation(RelationIndex(1));
				auto &joined = sets.Union(left, right);
				auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
				auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
				FilterInfo filter(CreateJoinComparison(a, b, ExpressionType::COMPARE_EQUAL), joined, 0);
				filter.SetLeftSet(left);
				filter.SetRightSet(right);
				filter.left_binding = a;
				filter.right_binding = b;
				JoinPredicateModel model;
				auto &predicate = model.RegisterPredicate(filter, JoinPredicateClass::INNER_EQUALITY, a, b);
				predicate.SetEqualityClassIndex(optional_idx(0));
				JoinEqualityClass equality;
				equality.index = 0;
				equality.columns = {a, b};
				equality.relations = {RelationIndex(0), RelationIndex(1)};
				equality.edges.emplace_back(predicate, RelationIndex(0), RelationIndex(1), a, b);
				model.AddEqualityClass(std::move(equality));
				CardinalityEstimator estimator(sets, model);

				auto left_stats = CreateStats({a}, {1}, 20);
				auto right_stats = CreateStats({b}, {bound}, 100);
				left_stats.columns[0].total_domain = DistinctCount(10, DistinctCountSource::MIN_MAX);
				right_stats.columns[0].total_domain = DistinctCount(10, source);
				left_stats.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(1);
				if (filter_both) {
					right_stats.columns[0].semi_anti_join_domain_evidence.TightenDomainBound(bound);
				} else {
					// A modeled current domain or uniqueness is not an explicit filter restriction.
					right_stats.columns[0].semi_anti_join_domain_evidence.MarkUnique();
				}
				estimator.Initialize({left_stats, right_stats});
				auto expected = filter_both && source != DistinctCountSource::CARDINALITY ? 2000 / bound : 200;
				REQUIRE(estimator.EstimateCardinality(joined).GetIndex() == expected);
			}
		}
	}
}

TEST_CASE("Large cardinality estimates saturate and preserve integer bounds", "[optimizer][relation_statistics]") {
	auto maximum = NumericLimits<idx_t>::Maximum();
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(0) == 0);
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(-1) == 0);
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(std::numeric_limits<double>::quiet_NaN()) == 0);
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(std::numeric_limits<double>::infinity()) == maximum);
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(static_cast<double>(maximum)) == maximum);
	REQUIRE(RelationStatisticsHelper::CardinalityFromDouble(20.4) == 20);

	vector<DistinctCount> domains {{maximum, DistinctCountSource::EXACT}, {maximum, DistinctCountSource::EXACT}};
	REQUIRE(RelationStatisticsHelper::EstimateDistinctCardinality(domains, maximum) == maximum);
	REQUIRE(RelationStatisticsHelper::EstimateDistinctCardinality(domains, maximum - 1) == maximum - 1);
	REQUIRE(RelationStatisticsHelper::EstimateCurrentDomain(domains[0], maximum, maximum - 1).distinct_count ==
	        maximum - 1);
}

TEST_CASE("Complete group counts retain marginal domains", "[optimizer][relation_statistics]") {
	for (auto source : {DistinctCountSource::EXACT, DistinctCountSource::HLL, DistinctCountSource::MIN_MAX,
	                    DistinctCountSource::CARDINALITY}) {
		for (idx_t rows : {idx_t(0), idx_t(1), idx_t(50), idx_t(10000)}) {
			vector<DistinctCount> domains {{50, source}};
			auto previous = RelationStatisticsHelper::EstimateDistinctCardinality(domains, rows);
			domains.emplace_back(1, source);
			REQUIRE(RelationStatisticsHelper::EstimateDistinctCardinality(domains, rows) == previous);
			for (idx_t count : {idx_t(2), idx_t(10), idx_t(10000)}) {
				domains.emplace_back(count, source);
				auto estimate = RelationStatisticsHelper::EstimateDistinctCardinality(domains, rows);
				REQUIRE(estimate >= previous);
				REQUIRE(estimate >= MinValue(count, rows));
				REQUIRE(estimate <= rows);
				previous = estimate;
			}
		}
	}
}

TEST_CASE("Implicit full grouping sets preserve complete group statistics", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto key = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto extra = ColumnBinding(TableIndex(10), ProjectionIndex(1));
	auto child_stats = CreateStats({key, extra}, {5, 5000}, 5000);
	LogicalAggregate aggregate(TableIndex(20), TableIndex(30), vector<unique_ptr<Expression>>());
	aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, key));
	auto key_only = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(key_only);
	REQUIRE(key_only->cardinality == 5);
	REQUIRE(*key_only->columns[0].GetSupportedSemiAntiDomainSize() == 5);

	aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, extra));
	auto implicit = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(implicit);
	REQUIRE(implicit->cardinality == 5000);
	REQUIRE_FALSE(implicit->columns[0].GetSupportedSemiAntiDomainSize());
	REQUIRE(implicit->columns[0].total_domain.distinct_count == 5);
	REQUIRE(implicit->columns[0].total_domain.source == DistinctCountSource::EXACT);
	REQUIRE(implicit->columns[1].current_domain.distinct_count == 5000);

	aggregate.grouping_sets.emplace_back();
	aggregate.grouping_sets[0].insert(ProjectionIndex(0));
	aggregate.grouping_sets[0].insert(ProjectionIndex(1));
	auto explicit_set = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child_stats, *connection.context);
	REQUIRE(explicit_set);
	REQUIRE(explicit_set->cardinality == implicit->cardinality);
	REQUIRE(explicit_set->columns[0].total_domain.distinct_count == implicit->columns[0].total_domain.distinct_count);
}

struct CardinalityProviderData : public TableFunctionData {
	NodeStatistics statistics {0, 0};
	shared_ptr<idx_t> calls = make_shared_ptr<idx_t>(0);
	shared_ptr<idx_t> column_calls = make_shared_ptr<idx_t>(0);
	vector<reference<LogicalGet>> observed_scans;
	shared_ptr<bool> captured_before_mutation = make_shared_ptr<bool>(true);
	DistinctCount domain {0, DistinctCountSource::CARDINALITY};
};

static unique_ptr<LogicalGet> CreateCardinalityProvider(optional_idx estimate, optional_idx maximum) {
	auto data = make_uniq<CardinalityProviderData>();
	data->statistics.has_estimated_cardinality = estimate.IsValid();
	data->statistics.has_max_cardinality = maximum.IsValid();
	if (estimate.IsValid()) {
		data->statistics.estimated_cardinality = estimate.GetIndex();
	}
	if (maximum.IsValid()) {
		data->statistics.max_cardinality = maximum.GetIndex();
	}
	TableFunction function("cardinality_provider", {}, nullptr);
	function.cardinality = [](ClientContext &, auto bind_data) -> unique_ptr<NodeStatistics> {
		auto &data = bind_data->template Cast<CardinalityProviderData>();
		(*data.calls)++;
		for (auto &scan : data.observed_scans) {
			if (scan.get().has_estimated_cardinality) {
				*data.captured_before_mutation = false;
			}
		}
		return make_uniq<NodeStatistics>(data.statistics);
	};
	auto get = make_uniq<LogicalGet>(TableIndex(10), BoundTableFunction(function), std::move(data),
	                                 vector<LogicalType> {LogicalType::INTEGER}, vector<Identifier> {"i"});
	get->AddColumnId(0);
	return get;
}

TEST_CASE("Scan filters acquire hidden input columns once and apply conjunction rows once",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto scan = CreateCardinalityProvider(100, 1000);
	scan->returned_types.push_back(LogicalType::INTEGER);
	scan->names.emplace_back("hidden");
	scan->AddColumnId(1);
	scan->projection_ids = {ProjectionIndex(0)};
	auto &data = scan->bind_data->Cast<CardinalityProviderData>();
	scan->function.statistics = [](ClientContext &, auto bind_data, column_t) -> unique_ptr<BaseStatistics> {
		auto &data = bind_data->template Cast<CardinalityProviderData>();
		(*data.column_calls)++;
		auto result = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
		result.SetDistinctCount(10);
		return result.ToUnique();
	};
	scan->table_filters.PushFilter(
	    ProjectionIndex(1), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::INTEGER(1)));
	scan->table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                       ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	RelationStatsExtractor extractor(*connection.context);
	auto result = extractor.Extract(*scan);
	REQUIRE(result);
	REQUIRE(result->columns.size() == 1);
	REQUIRE(result->cardinality == 10);
	REQUIRE(result->row_retention.GetDouble() == Approx(0.1));
	REQUIRE(result->columns[0].current_domain.distinct_count ==
	        RelationStatisticsHelper::EstimateCurrentDomain(DistinctCount(10, DistinctCountSource::HLL), 100, 10)
	            .distinct_count);
	REQUIRE(extractor.Extract(*scan));
	REQUIRE(*data.column_calls == 2);
	REQUIRE(*data.calls == 1);
}

static unique_ptr<LogicalOperator> PlanStatisticsQuery(Connection &connection, const string &query) {
	Parser parser(*connection.context);
	parser.ParseQuery(query);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	planner.plan->ResolveOperatorTypes();
	return std::move(planner.plan);
}

static void SetStatisticsTestCardinalities(LogicalOperator &op) {
	op.SetEstimatedCardinality(0);
	for (auto &child : op.children) {
		SetStatisticsTestCardinalities(*child);
	}
}

static void CreateStatisticsJoinTables(Connection &connection) {
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE TABLE stats_a AS SELECT i::INTEGER x, (i % 10)::INTEGER k FROM range(1000) t(i)"));
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE TABLE stats_b AS SELECT i::INTEGER y, (i % 10)::INTEGER k FROM range(1000) t(i)"));
	REQUIRE_NO_FAIL(
	    connection.Query("CREATE TABLE stats_c AS SELECT i::INTEGER z, (i % 10)::INTEGER k FROM range(100) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='join_order'"));
}

static unique_ptr<LogicalGet> CreateDomainProvider(TableIndex index, idx_t rows, DistinctCount domain) {
	auto get = CreateCardinalityProvider(rows, rows);
	get->table_index = index;
	get->bind_data.write().Cast<CardinalityProviderData>().domain = domain;
	get->function.statistics = [](ClientContext &, auto bind_data, column_t) -> unique_ptr<BaseStatistics> {
		auto &domain = bind_data->template Cast<CardinalityProviderData>().domain;
		auto result = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
		if (domain.source == DistinctCountSource::MIN_MAX) {
			NumericStats::SetMin(result, Value::INTEGER(0));
			NumericStats::SetMax(result, Value::INTEGER(NumericCast<int32_t>((domain.distinct_count - 1).GetIndex())));
		} else {
			result.SetDistinctCount(domain.distinct_count.GetIndex());
		}
		return result.ToUnique();
	};
	return get;
}

TEST_CASE("Equality filter estimates retain their ceiling at scan and filter boundaries",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t rows : {idx_t(113), idx_t(210)}) {
		auto scan = CreateDomainProvider(TableIndex(10), rows, DistinctCount(105, DistinctCountSource::HLL));
		auto child = RelationStatisticsHelper::ExtractGetStats(*scan, *connection.context, rows);
		auto binding = scan->GetColumnBindings()[0];
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
		    make_uniq<BoundConstantExpression>(Value::INTEGER(42))));
		auto filtered = RelationStatisticsHelper::ConditionFilterStats(child, expressions, {binding});
		REQUIRE(filtered.cardinality == 2);
		scan->table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
		                                                       ExpressionType::COMPARE_EQUAL, Value::INTEGER(42)));
		auto pushed = RelationStatisticsHelper::ExtractGetStats(*scan, *connection.context, rows);
		REQUIRE(pushed.cardinality == filtered.cardinality);
	}
}

TEST_CASE("Equality filter compatibility preserves fractional and scaled arithmetic",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	ColumnBinding binding(TableIndex(10), ProjectionIndex(0));
	auto estimate = [&](CardinalityValue rows, CardinalityValue ndv, ExpressionType type,
	                    DistinctCountSource source = DistinctCountSource::HLL) {
		RelationStats child;
		child.cardinality = rows;
		child.columns.emplace_back(binding, DistinctCount(ndv, source), DistinctCount(MinValue(rows, ndv), source),
		                           Identifier("key"));
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(
		    BoundComparisonExpression::Create(type, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
		                                      make_uniq<BoundConstantExpression>(Value::INTEGER(42))));
		auto result = RelationStatisticsHelper::ConditionFilterStats(child, expressions, {binding});
		REQUIRE(result.cardinality <= rows);
		return result.cardinality;
	};
	auto equal = ExpressionType::COMPARE_EQUAL;
	REQUIRE(estimate(0.5, 1, equal) == 0.5);
	REQUIRE(estimate(2.5, 1, equal) == 2.5);
	CardinalityValue small(std::ldexp(1.0, -600));
	CardinalityValue large(std::ldexp(1.0, 600));
	for (auto rows : {small * small, large, large * large}) {
		auto expected = rows < 1 ? rows : rows / 3;
		REQUIRE(estimate(rows, 3, equal) == expected);
	}
	REQUIRE(estimate(113, 105, ExpressionType::COMPARE_NOTEQUAL).GetDouble() == Approx(113.0 * 104 / 105));
	REQUIRE(estimate(113, 105, equal, DistinctCountSource::CARDINALITY).GetDouble() == Approx(22.6));
}

TEST_CASE("Grouped membership statistics reach enumeration and parent cost before reconstruction",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto left = CreateDomainProvider(TableIndex(10), 64, DistinctCount(59, DistinctCountSource::HLL));
	auto keys = CreateDomainProvider(TableIndex(20), 128, DistinctCount(59, DistinctCountSource::HLL));
	auto group = make_uniq<LogicalAggregate>(TableIndex(30), TableIndex(31), vector<unique_ptr<Expression>>());
	group->groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, keys->GetColumnBindings()[0]));
	group->children.push_back(std::move(keys));
	auto semi = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	semi->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left->GetColumnBindings()[0]),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, group->GetColumnBindings()[0]),
	    ExpressionType::COMPARE_EQUAL);
	semi->children.push_back(std::move(left));
	semi->children.push_back(std::move(group));
	auto right = CreateDomainProvider(TableIndex(40), 128, DistinctCount(59, DistinctCountSource::HLL));
	auto outer = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	outer->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, semi->GetColumnBindings()[0]),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right->GetColumnBindings()[0]),
	    ExpressionType::COMPARE_EQUAL);
	outer->children.push_back(std::move(semi));
	outer->children.push_back(std::move(right));
	unique_ptr<LogicalOperator> plan = std::move(outer);
	plan->ResolveOperatorTypes();
	JoinOrderOptimizer optimizer(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE(graph.Build(optimizer, *plan));
	CardinalityEstimator estimator(graph.set_manager, graph.GetPredicateModel());
	CostModel cost(estimator);
	PlanEnumerator enumeration(graph, cost, graph.GetQueryGraphEdges());
	enumeration.InitLeafPlans();
	REQUIRE(enumeration.SolveJoinOrder());
	auto &plans = enumeration.GetPlans();
	bool observed_membership = false;
	for (auto &entry : plans) {
		auto &node = *entry.second;
		if (node.is_leaf) {
			continue;
		}
		auto &lhs = *plans.at(node.left_set);
		auto &rhs = *plans.at(node.right_set);
		REQUIRE(node.cost == Approx(cost.ComputeCost(lhs, rhs, node.join_operator, node.set)));
		if (node.join_operator && node.join_operator->type == JoinOrderOperatorType::SEMI && node.set.count == 2) {
			observed_membership = true;
			REQUIRE(node.cardinality.GetDouble() == Approx(64));
			REQUIRE(node.cost == Approx(64));
		}
	}
	REQUIRE(observed_membership);
	unordered_set<RelationIndex> all;
	for (idx_t i = 0; i < graph.relation_manager.NumRelations(); i++) {
		all.insert(RelationIndex(i));
	}
	auto &set = graph.set_manager.GetJoinRelation(all);
	auto before = graph.ProjectStats(estimator, set, plan->GetColumnBindings());
	REQUIRE(before);
	REQUIRE(before->cardinality.GetDouble() == Approx(64.0 * 128 / 59));
	REQUIRE(plans.at(set)->cardinality == before->cardinality);
	graph.plans = plans;
	auto reconstructed = graph.Reconstruct(std::move(plan), estimator);
	REQUIRE(reconstructed.stats.cardinality == before->cardinality);
	REQUIRE(reconstructed.op->estimated_cardinality == before->GetCardinalityForPlan());
	// Moving the ON expressions must not invalidate the compiled region summary.
	REQUIRE(estimator.EstimateCardinality(set) == before->cardinality);
}

TEST_CASE("Fractional rows and domains survive materialized producer handoff",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	DuckDB db;
	Connection connection(db);
	for (bool materialized : {false, true}) {
		auto left = CreateDomainProvider(TableIndex(10), 2, DistinctCount(4, DistinctCountSource::HLL));
		auto right = CreateDomainProvider(TableIndex(20), 1, DistinctCount(4, DistinctCountSource::HLL));
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->conditions.emplace_back(
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left->GetColumnBindings()[0]),
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right->GetColumnBindings()[0]),
		    ExpressionType::COMPARE_EQUAL);
		join->children.push_back(std::move(left));
		join->children.push_back(std::move(right));
		unique_ptr<LogicalOperator> plan = std::move(join);
		if (materialized) {
			auto ref = make_uniq<LogicalCTERef>(TableIndex(40), TableIndex(30),
			                                    vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
			                                    vector<Identifier> {"a", "b"});
			plan = make_uniq<LogicalMaterializedCTE>(Identifier("fractional"), TableIndex(30), 2, std::move(plan),
			                                         std::move(ref), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
		}
		plan->ResolveOperatorTypes();
		RelationStatsExtractor extractor(*connection.context);
		auto extracted = extractor.Extract(*plan);
		REQUIRE(extracted);
		REQUIRE(extracted->cardinality == 0.5);
		REQUIRE(extracted->columns[0].current_domain.distinct_count == 0.5);
		JoinOrderOptimizer optimizer(*connection.context);
		RelationStats optimized;
		plan = optimizer.Optimize(std::move(plan), optimized);
		REQUIRE(optimized.stats_initialized);
		REQUIRE(optimized.cardinality == extracted->cardinality);
		REQUIRE(optimized.columns[0].current_domain.distinct_count ==
		        extracted->columns[0].current_domain.distinct_count);
		REQUIRE(plan->estimated_cardinality == 1);
	}
}

TEST_CASE("Lazy membership projection owns predicate metadata after expressions move",
          "[optimizer][relation_statistics]") {
	for (bool residual : {false, true}) {
		for (auto join_type : {JoinType::SEMI, JoinType::ANTI}) {
			JoinRelationSetManager sets;
			auto &left = sets.GetJoinRelation(RelationIndex(0));
			auto &right = sets.GetJoinRelation(RelationIndex(1));
			auto &root = sets.Union(left, right);
			auto a = ColumnBinding(TableIndex(0), ProjectionIndex(0));
			auto payload = ColumnBinding(TableIndex(0), ProjectionIndex(1));
			auto b = ColumnBinding(TableIndex(1), ProjectionIndex(0));
			auto comparison = residual ? ExpressionType::COMPARE_GREATERTHAN : ExpressionType::COMPARE_EQUAL;
			FilterInfo filter(CreateJoinComparison(a, b, comparison), root, 0, join_type);
			filter.source_operator_index = optional_idx(0);
			filter.SetLeftSet(left);
			filter.SetRightSet(right);
			filter.semantic_left_set = left;
			filter.semantic_right_set = right;
			JoinPredicateModel model;
			model.RegisterPredicate(filter, JoinPredicateClass::SEMI_ANTI_JOIN, a, b);
			auto lhs = CreateStats({a, payload}, {100, 20}, 100);
			auto rhs = CreateStats({b}, {50}, 50);
			rhs.columns[0].semi_anti_join_domain_evidence.MarkUnique();
			vector<unique_ptr<JoinOrderOperator>> operators;
			AddOperator(operators, sets, left, right,
			            join_type == JoinType::SEMI ? JoinOrderOperatorType::SEMI : JoinOrderOperatorType::ANTI);
			CardinalityEstimator reference(sets, model);
			reference.Initialize({lhs, rhs}, operators);
			auto before = reference.ProjectStats(root, {payload});
			REQUIRE(before);
			REQUIRE(before->cardinality == (residual ? 20 : 50));
			CardinalityEstimator deferred(sets, model);
			deferred.Initialize({lhs, rhs}, operators);
			deferred.EstimateCardinality(root);
			// Only the membership keys have been projected. The payload is requested for
			// the first time after reconstruction would have consumed the expression.
			auto moved_expression = std::move(filter.filter);
			auto after = deferred.ProjectStats(root, {payload});
			REQUIRE(after);
			REQUIRE(after->cardinality == before->cardinality);
			REQUIRE(after->columns[0].current_domain.distinct_count ==
			        before->columns[0].current_domain.distinct_count);
			REQUIRE(after->row_retention == before->row_retention);
		}
	}
}

TEST_CASE("Underflowing positive statistics survive grouping materialization and membership",
          "[optimizer][relation_statistics][cardinality_arithmetic]") {
	DuckDB db;
	Connection connection(db);
	const idx_t domain = 1000000000000000000;
	auto first = CreateDomainProvider(TableIndex(100), 2, DistinctCount(domain, DistinctCountSource::HLL));
	auto key = first->GetColumnBindings()[0];
	unique_ptr<LogicalOperator> input = std::move(first);
	CardinalityValue expected = 2;
	for (idx_t i = 1; i < 32; i++) {
		auto next = CreateDomainProvider(TableIndex(100 + i), 2, DistinctCount(domain, DistinctCountSource::HLL));
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->conditions.emplace_back(
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, key),
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, next->GetColumnBindings()[0]),
		    ExpressionType::COMPARE_EQUAL);
		join->children.push_back(std::move(input));
		join->children.push_back(std::move(next));
		input = std::move(join);
		expected = expected * 2 / CardinalityValue(domain);
	}
	REQUIRE_FALSE(expected.IsZero());
	REQUIRE(expected.GetDouble() == 0);
	auto group = make_uniq<LogicalAggregate>(TableIndex(1000), TableIndex(1001), vector<unique_ptr<Expression>>());
	group->groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, key));
	group->children.push_back(std::move(input));
	auto ref = make_uniq<LogicalCTERef>(TableIndex(2001), TableIndex(2000), vector<LogicalType> {LogicalType::INTEGER},
	                                    vector<Identifier> {"k"});
	auto right = CreateDomainProvider(TableIndex(3000), 1, DistinctCount(domain, DistinctCountSource::HLL));
	auto rhs_group = make_uniq<LogicalAggregate>(TableIndex(3001), TableIndex(3002), vector<unique_ptr<Expression>>());
	rhs_group->groups.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, right->GetColumnBindings()[0]));
	rhs_group->children.push_back(std::move(right));
	auto semi = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	semi->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ref->GetColumnBindings()[0]),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, rhs_group->GetColumnBindings()[0]),
	    ExpressionType::COMPARE_EQUAL);
	semi->children.push_back(std::move(ref));
	semi->children.push_back(std::move(rhs_group));
	unique_ptr<LogicalOperator> plan =
	    make_uniq<LogicalMaterializedCTE>(Identifier("tiny"), TableIndex(2000), 1, std::move(group), std::move(semi),
	                                      CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	plan->ResolveOperatorTypes();
	RelationStatsExtractor extractor(*connection.context);
	auto producer = extractor.Extract(*plan->children[0]);
	REQUIRE(producer);
	REQUIRE((producer->cardinality / expected).GetDouble() == Approx(1));
	REQUIRE((producer->columns[0].current_domain.distinct_count / expected).GetDouble() == Approx(1));
	auto extracted = extractor.Extract(*plan);
	REQUIRE(extracted);
	REQUIRE_FALSE(extracted->cardinality.IsZero());
	REQUIRE(extracted->cardinality.GetDouble() == 0);
	REQUIRE_FALSE(extracted->columns[0].current_domain.distinct_count.IsZero());
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats optimized;
	plan = optimizer.Optimize(std::move(plan), optimized);
	REQUIRE(optimized.stats_initialized);
	REQUIRE((optimized.cardinality / extracted->cardinality).GetDouble() == Approx(1));
	REQUIRE((optimized.columns[0].current_domain.distinct_count / extracted->columns[0].current_domain.distinct_count)
	            .GetDouble() == Approx(1));
	REQUIRE(plan->estimated_cardinality == 1);
}

TEST_CASE("Provider estimate zero and provider maximum zero have different contracts",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (bool empty_proof : {false, true}) {
		auto scan = CreateCardinalityProvider(optional_idx(0), empty_proof ? optional_idx(0) : optional_idx());
		auto &data = scan->bind_data->Cast<CardinalityProviderData>();
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*scan);
		REQUIRE(stats);
		REQUIRE(stats->IsEmpty() == empty_proof);
		REQUIRE(stats->GetCardinalityForPlan() == (empty_proof ? 0 : 1));
		REQUIRE(stats->GetCardinalityForCost() == (empty_proof ? 0 : 1));
		REQUIRE(*data.calls == 1);
	}
}

TEST_CASE("Provider points are reconciled without weakening source bounds", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (auto maximum : {optional_idx(0), optional_idx(5), optional_idx()}) {
		for (idx_t estimate : {idx_t(1), idx_t(1000)}) {
			auto scan = CreateCardinalityProvider(estimate, maximum);
			auto source = scan->EstimateSourceStatistics(*connection.context);
			REQUIRE(source.has_estimated_cardinality);
			REQUIRE(source.estimated_cardinality == estimate);
			REQUIRE(source.has_max_cardinality == maximum.IsValid());
			if (maximum.IsValid()) {
				REQUIRE(source.max_cardinality == maximum.GetIndex());
			}
			auto expected = maximum.IsValid() ? MinValue(estimate, maximum.GetIndex()) : estimate;
			auto stats = RelationStatisticsHelper::ExtractGetStats(*scan, *connection.context, source);
			REQUIRE(stats.cardinality == expected);
			REQUIRE(stats.GetCardinalityForCost() == expected);
			REQUIRE(stats.GetCardinalityForPlan() == expected);
			REQUIRE(stats.max_cardinality.IsValid() == maximum.IsValid());
			if (maximum.IsValid()) {
				REQUIRE(stats.max_cardinality.GetIndex() == maximum.GetIndex());
			}
			JoinOrderOptimizer optimizer(*connection.context);
			RelationStats optimized;
			auto plan = optimizer.Optimize(std::move(scan), optimized);
			REQUIRE(optimized.stats_initialized);
			REQUIRE(optimized.GetCardinalityForCost() == expected);
			REQUIRE(plan->estimated_cardinality == expected);
			REQUIRE(source.estimated_cardinality == estimate);
		}
	}
}

TEST_CASE("Empty proofs propagate across projected materialized and aggregate boundaries",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	vector<pair<string, idx_t>> cases {
	    {"WITH e AS MATERIALIZED (SELECT i FROM range(3) r(i) LIMIT 0) SELECT i+1 FROM e", 0},
	    {"WITH e AS MATERIALIZED (SELECT i FROM range(3) r(i) LIMIT 0) SELECT i FROM e GROUP BY i", 0},
	    {"WITH e AS MATERIALIZED (SELECT i FROM range(3) r(i) LIMIT 0) SELECT count(*) FROM e", 1},
	    {"SELECT * FROM range(3) a(i) SEMI JOIN (SELECT * FROM range(3) LIMIT 0) b ON a.i=b.range", 0},
	    {"SELECT * FROM range(3) a(i) ANTI JOIN (SELECT * FROM range(3) LIMIT 0) b ON a.i=b.range", 3},
	    {"SELECT * FROM range(3) a(i) LEFT JOIN (SELECT * FROM range(3) LIMIT 0) b ON a.i=b.range", 3},
	    {"SELECT * FROM (SELECT * FROM range(3) LIMIT 0) a FULL JOIN range(3) b ON a.range=b.range", 3}};
	for (auto &entry : cases) {
		CAPTURE(entry.first);
		auto plan = PlanStatisticsQuery(connection, entry.first);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*plan);
		REQUIRE(stats);
		REQUIRE(stats->GetCardinalityForPlan() == entry.second);
		REQUIRE(stats->IsEmpty() == (entry.second == 0));
		JoinOrderOptimizer optimizer(*connection.context);
		RelationStats optimized;
		plan = optimizer.Optimize(std::move(plan), optimized);
		REQUIRE(optimized.stats_initialized);
		REQUIRE(optimized.IsEmpty() == stats->IsEmpty());
		REQUIRE(optimized.GetCardinalityForPlan() == stats->GetCardinalityForPlan());
	}
	connection.Rollback();
}

static unique_ptr<LogicalOperator> CreateContributorJoin(bool projected, idx_t second_domain) {
	auto first = CreateDomainProvider(TableIndex(10), 10000, DistinctCount(50, DistinctCountSource::HLL));
	auto second = CreateDomainProvider(TableIndex(20), 10000, DistinctCount(second_domain, DistinctCountSource::HLL));
	auto first_binding = first->GetColumnBindings()[0];
	auto inner = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	inner->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, first_binding),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, second->GetColumnBindings()[0]),
	    ExpressionType::COMPARE_EQUAL);
	inner->children.push_back(std::move(first));
	inner->children.push_back(std::move(second));
	unique_ptr<LogicalOperator> left = std::move(inner);
	if (projected) {
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, first_binding));
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
		auto projection = make_uniq<LogicalProjection>(TableIndex(30), std::move(expressions));
		projection->children.push_back(std::move(left));
		left = std::move(projection);
	}
	auto dimension = CreateDomainProvider(TableIndex(40), 1000, DistinctCount(1000, DistinctCountSource::MIN_MAX));
	auto outer = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	outer->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left->GetColumnBindings()[0]),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, dimension->GetColumnBindings()[0]),
	    ExpressionType::COMPARE_EQUAL);
	outer->children.push_back(std::move(left));
	outer->children.push_back(std::move(dimension));
	return std::move(outer);
}

TEST_CASE("Projected joins use visible domains and retain independent source guards",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (auto second_domain : {idx_t(50), idx_t(100)}) {
		for (bool projected : {false, true}) {
			auto plan = CreateContributorJoin(projected, second_domain);
			JoinOrderSourceStats snapshot(*connection.context, *plan);
			auto sources = snapshot.Resolve(*plan->children[0]);
			auto &lineage = sources.at(plan->children[0]->GetColumnBindings()[0]);
			REQUIRE(lineage.equivalents.size() == 1);
			REQUIRE(lineage.direct.binding.table_index == TableIndex(10));
			REQUIRE(lineage.equivalents[0].binding.table_index == TableIndex(20));
			snapshot.Resolve(*plan);
			REQUIRE(
			    snapshot.Resolve(*plan->children[0]).at(plan->children[0]->GetColumnBindings()[0]).equivalents.size() ==
			    1);
			auto expected = idx_t(100000000000) / (second_domain * (projected ? 50 : second_domain));
			RelationStatsExtractor extractor(*connection.context);
			auto stats = extractor.Extract(*plan);
			REQUIRE(stats);
			REQUIRE(stats->cardinality == expected);

			JoinOrderOptimizer optimizer(*connection.context);
			RelationStats optimized_stats;
			auto optimized = optimizer.Optimize(CreateContributorJoin(projected, second_domain), optimized_stats);
			REQUIRE(optimized_stats.stats_initialized);
			REQUIRE(optimized_stats.cardinality == expected);
		}
	}
}

TEST_CASE("Aliases created before joins retain hidden contributors", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto plan = CreateContributorJoin(true, 100);
	auto &outer_projection = plan->children[0]->Cast<LogicalProjection>();
	auto &inner = outer_projection.children[0]->Cast<LogicalComparisonJoin>();
	auto original_binding = inner.children[0]->GetColumnBindings()[0];
	vector<unique_ptr<Expression>> aliases;
	aliases.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, original_binding));
	aliases.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, original_binding));
	auto alias_projection = make_uniq<LogicalProjection>(TableIndex(15), std::move(aliases));
	alias_projection->children.push_back(std::move(inner.children[0]));
	inner.children[0] = std::move(alias_projection);
	inner.conditions[0].LeftReference() = inner.conditions[0].GetLHS().Cast<BoundColumnRefExpression>().WithBinding(
	    ColumnBinding(TableIndex(15), ProjectionIndex(0)));
	outer_projection.expressions[0] = outer_projection.expressions[0]->Cast<BoundColumnRefExpression>().WithBinding(
	    ColumnBinding(TableIndex(15), ProjectionIndex(1)));
	JoinOrderSourceStats snapshot(*connection.context, *plan);
	auto sources = snapshot.Resolve(outer_projection);
	REQUIRE(sources.at(outer_projection.GetColumnBindings()[0]).equivalents.size() == 1);
	REQUIRE(sources.at(outer_projection.GetColumnBindings()[0]).direct.binding == original_binding);
	RelationStatsExtractor extractor(*connection.context);
	auto stats = extractor.Extract(*plan);
	REQUIRE(stats);
	REQUIRE(stats->cardinality == 20000000);
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats optimized_stats;
	plan = optimizer.Optimize(std::move(plan), optimized_stats);
	REQUIRE(optimized_stats.stats_initialized);
	REQUIRE(optimized_stats.cardinality == 20000000);
}

TEST_CASE("Projected source equalities exclude correlated operands", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (bool cast : {false, true}) {
		auto plan = CreateContributorJoin(true, 100);
		auto &projection = *plan->children[0];
		auto &join = projection.children[0]->Cast<LogicalComparisonJoin>();
		auto &condition = join.conditions[0];
		auto binding = condition.GetLHS().Cast<BoundColumnRefExpression>().Binding();
		condition.LeftReference() = condition.GetLHS().Cast<BoundColumnRefExpression>().WithDepth(1);
		if (cast) {
			condition.LeftReference() = BoundCastExpression::AddCastToType(
			    *connection.context, std::move(condition.LeftReference()), LogicalType::BIGINT);
			condition.RightReference() = BoundCastExpression::AddCastToType(
			    *connection.context, std::move(condition.RightReference()), LogicalType::BIGINT);
		}
		JoinOrderSourceStats snapshot(*connection.context, projection);
		auto sources = snapshot.Resolve(projection);
		auto &contributors = sources.at(projection.GetColumnBindings()[0]);
		REQUIRE(contributors.equivalents.empty());
		REQUIRE(contributors.direct.binding == binding);
	}
}

TEST_CASE("Join source snapshots retain scan populations and resolve relation boundaries",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	REQUIRE_NO_FAIL(connection.Query("ANALYZE"));
	connection.BeginTransaction();
	vector<pair<string, idx_t>> cases {
	    {"SELECT x AS a, x AS b, k + 1 AS c FROM stats_a", 2},
	    {"SELECT a.x, b.y, a.k + b.k AS c FROM stats_a a JOIN stats_b b USING(k)", 2},
	    {"SELECT x FROM stats_a WHERE k = 1 ORDER BY x", 1},
	    {"SELECT a.x, b.y FROM stats_a a LEFT JOIN stats_b b USING(k)", 1},
	    {"SELECT a.x FROM stats_a a SEMI JOIN stats_b b USING(k)", 1},
	    {"SELECT * FROM (SELECT x FROM stats_a LIMIT 10)", 0},
	    {"SELECT x::VARCHAR FROM stats_a", 0},
	    {"SELECT k, count(*) FROM stats_a GROUP BY k", 0},
	    {"SELECT x FROM stats_a UNION ALL SELECT y FROM stats_b", 0},
	    {"SELECT * FROM (VALUES (1), (2), (3)) v(x)", 0},
	};
	for (auto &entry : cases) {
		CAPTURE(entry.first);
		auto plan = PlanStatisticsQuery(connection, entry.first);
		JoinOrderSourceStats snapshot(*connection.context, *plan);
		auto sources = snapshot.Resolve(*plan);
		REQUIRE(sources.size() == entry.second);
		for (auto &source : sources) {
			REQUIRE(source.second.direct.source_cardinality == 1000);
			for (auto &contributor : source.second.equivalents) {
				REQUIRE(contributor.source_cardinality == 1000);
			}
		}
	}
	auto plan = PlanStatisticsQuery(connection, "SELECT x AS a, x AS b FROM stats_a");
	JoinOrderSourceStats snapshot(*connection.context, *plan);
	auto sources = snapshot.Resolve(*plan);
	auto bindings = plan->GetColumnBindings();
	REQUIRE(sources.at(bindings[0]).equivalents.empty());
	REQUIRE(sources.at(bindings[1]).equivalents.empty());
	REQUIRE(sources.at(bindings[0]).direct.binding == sources.at(bindings[1]).direct.binding);

	auto self_join = PlanStatisticsQuery(connection, "SELECT a.x, b.x FROM stats_a a JOIN stats_a b USING(k)");
	JoinOrderSourceStats self_snapshot(*connection.context, *self_join);
	auto self_sources = self_snapshot.Resolve(*self_join);
	auto self_bindings = self_join->GetColumnBindings();
	REQUIRE(self_sources.at(self_bindings[0]).equivalents.empty());
	REQUIRE(self_sources.at(self_bindings[1]).equivalents.empty());
	REQUIRE(self_sources.at(self_bindings[0]).direct.binding.table_index !=
	        self_sources.at(self_bindings[1]).direct.binding.table_index);
	vector<pair<string, idx_t>> contributor_cases {
	    {"SELECT a.k FROM stats_a a JOIN stats_b b USING(k)", 2},
	    {"SELECT a.k FROM stats_a a, stats_b b WHERE a.k=b.k", 2},
	    {"SELECT a.k FROM stats_a a JOIN stats_b b ON a.k::BIGINT IS NOT DISTINCT FROM b.k::BIGINT", 2},
	    {"SELECT a.k FROM stats_a a, stats_b b WHERE a.k=b.k AND b.k=b.y", 3},
	    {"SELECT a.k FROM stats_a a, stats_b b WHERE a.k=b.k OR a.x=b.y", 1},
	    {"SELECT a.k FROM stats_a a LEFT JOIN stats_b b USING(k)", 1},
	    {"SELECT p.a,p.b FROM (SELECT a.k AS a,a.k AS b FROM stats_a a JOIN stats_b b USING(k)) p", 2},
	    {"SELECT p.b FROM (SELECT k AS a,k AS b FROM stats_a) p JOIN stats_b b ON p.a=b.k", 2},
	};
	for (auto &entry : contributor_cases) {
		CAPTURE(entry.first);
		auto input = PlanStatisticsQuery(connection, entry.first);
		JoinOrderSourceStats captured(*connection.context, *input);
		auto output = captured.Resolve(*input);
		for (auto &binding : input->GetColumnBindings()) {
			REQUIRE(output.at(binding).equivalents.size() + 1 == entry.second);
			column_binding_set_t origins {output.at(binding).direct.binding};
			for (auto &source : output.at(binding).equivalents) {
				origins.insert(source.binding);
			}
			REQUIRE(origins.size() == entry.second);
		}
	}
	auto computed =
	    PlanStatisticsQuery(connection, "SELECT p.k FROM (SELECT x%10 AS k FROM stats_a) p JOIN stats_b b USING(k)");
	JoinOrderSourceStats computed_snapshot(*connection.context, *computed);
	REQUIRE(computed_snapshot.Resolve(*computed).empty());
	connection.Rollback();
}

TEST_CASE("Join source snapshots precede child optimization and reuse provider statistics",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto scan = CreateCardinalityProvider(100, 1000);
	auto &data = scan->bind_data->Cast<CardinalityProviderData>();
	auto calls = data.calls;
	auto column_calls = data.column_calls;
	scan->function.statistics = [](ClientContext &, auto bind_data, column_t) -> unique_ptr<BaseStatistics> {
		auto &data = bind_data->template Cast<CardinalityProviderData>();
		(*data.column_calls)++;
		auto result = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
		result.SetDistinctCount(10);
		return result.ToUnique();
	};
	scan->SetEstimatedCardinality(7);
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(10), ProjectionIndex(0))));
	auto projection = make_uniq<LogicalProjection>(TableIndex(20), std::move(expressions));
	projection->children.push_back(std::move(scan));
	JoinOrderOptimizer optimizer(*connection.context);
	auto plan = optimizer.Optimize(std::move(projection));
	auto sources = optimizer.GetSourceStats()->Resolve(*plan);
	REQUIRE(sources.size() == 1);
	REQUIRE(sources.begin()->second.equivalents.empty());
	auto &source = sources.begin()->second.direct;
	REQUIRE(source.source_cardinality == 100);
	REQUIRE(source.domain.distinct_count == 10);
	REQUIRE(*column_calls == 1);
	REQUIRE(*calls == 1);
	plan->SetEstimatedCardinality(1);
	REQUIRE(optimizer.GetSourceStats()->Resolve(*plan).begin()->second.direct.source_cardinality == 100);
}

TEST_CASE("Join source collection visits both subtrees before either is optimized",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto left = CreateCardinalityProvider(100, 100);
	auto right = CreateCardinalityProvider(200, 200);
	right->table_index = TableIndex(11);
	auto &left_data = left->bind_data.write().Cast<CardinalityProviderData>();
	auto &right_data = right->bind_data.write().Cast<CardinalityProviderData>();
	left_data.observed_scans = {*left, *right};
	right_data.observed_scans = {*left, *right};
	auto left_captured = left_data.captured_before_mutation;
	auto right_captured = right_data.captured_before_mutation;
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	join->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(10), ProjectionIndex(0))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(11), ProjectionIndex(0))),
	    ExpressionType::COMPARE_EQUAL);
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	JoinOrderOptimizer optimizer(*connection.context);
	auto plan = optimizer.Optimize(std::move(join));
	REQUIRE(*left_captured);
	REQUIRE(*right_captured);
	REQUIRE(optimizer.GetSourceStats()->Resolve(*plan).size() == 2);
}

TEST_CASE("Join source provenance respects secure views and scan samples", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (bool propagate : {false, true}) {
		auto input = LogicalProjection::CreateIdentity(TableIndex(20), CreateCardinalityProvider(100, 100));
		auto view = make_uniq<LogicalSecureView>("source_view", std::move(input));
		view->propagate_statistics = propagate;
		auto plan = LogicalProjection::CreateIdentity(TableIndex(30), std::move(view));
		JoinOrderSourceStats snapshot(*connection.context, *plan);
		REQUIRE(snapshot.Resolve(*plan).size() == (propagate ? 1 : 0));
		REQUIRE(snapshot.Resolve(*plan->children[0]).size() == (propagate ? 1 : 0));
		REQUIRE(snapshot.Resolve(*plan->children[0]->children[0]).size() == 1);
	}
	auto scan = CreateCardinalityProvider(100, 100);
	scan->extra_info.sample_options = make_uniq<SampleOptions>();
	scan->extra_info.sample_options->sample_size = Value::BIGINT(10);
	scan->extra_info.sample_options->is_percentage = false;
	JoinOrderSourceStats snapshot(*connection.context, *scan);
	REQUIRE(snapshot.FindScanStats(*scan));
	REQUIRE(snapshot.Resolve(*scan).empty());
	auto projected = LogicalProjection::CreateIdentity(TableIndex(20), std::move(scan));
	snapshot.AddPlan(*connection.context, *projected);
	REQUIRE(snapshot.Resolve(*projected).empty());
}

class SourceCountingProjection : public LogicalProjection {
public:
	SourceCountingProjection(TableIndex index, vector<unique_ptr<Expression>> expressions)
	    : LogicalProjection(index, std::move(expressions)) {
	}

	idx_t binding_calls = 0;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		binding_calls++;
		return LogicalProjection::GetColumnBindings();
	}
};

TEST_CASE("Join source collection resolves each nested projection once", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (bool computed : {false, true}) {
		auto scan = CreateCardinalityProvider(100, 100);
		auto calls = scan->bind_data->Cast<CardinalityProviderData>().calls;
		unique_ptr<LogicalOperator> plan = std::move(scan);
		vector<reference<SourceCountingProjection>> projections;
		for (idx_t i = 0; i < 8; i++) {
			vector<unique_ptr<Expression>> expressions;
			if (computed && i == 0) {
				expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
			} else {
				expressions.push_back(
				    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, plan->GetColumnBindings()[0]));
			}
			auto projection = make_uniq<SourceCountingProjection>(TableIndex(20 + i), std::move(expressions));
			projections.push_back(*projection);
			projection->children.push_back(std::move(plan));
			plan = std::move(projection);
		}
		for (auto &projection : projections) {
			projection.get().binding_calls = 0;
		}
		JoinOrderSourceStats snapshot(*connection.context, *plan);
		for (auto &projection : projections) {
			auto sources = snapshot.Resolve(projection.get());
			REQUIRE(sources.size() == (computed ? 0 : 1));
			sources.clear();
			REQUIRE(snapshot.Resolve(projection.get()).size() == (computed ? 0 : 1));
			REQUIRE(projection.get().binding_calls == 1);
		}
		snapshot.AddPlan(*connection.context, *plan);
		REQUIRE(*calls == 1);
		for (auto &projection : projections) {
			REQUIRE(projection.get().binding_calls == 1);
		}
	}
}

TEST_CASE("Join source summaries support new projections and appended roots without bypassing barriers",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto first = LogicalProjection::CreateIdentity(TableIndex(20), CreateCardinalityProvider(100, 100));
	auto &captured = *first;
	auto calls = first->children[0]->Cast<LogicalGet>().bind_data->Cast<CardinalityProviderData>().calls;
	JoinOrderSourceStats snapshot(*connection.context, *first);
	auto projected = LogicalProjection::CreateIdentity(TableIndex(30), std::move(first));
	REQUIRE(snapshot.Resolve(*projected).begin()->second.direct.source_cardinality == 100);
	auto limit = make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(10), BoundLimitNode());
	limit->children.push_back(std::move(projected));
	REQUIRE(snapshot.Resolve(*limit).empty());
	auto outer = LogicalProjection::CreateIdentity(TableIndex(40), std::move(limit));
	REQUIRE(snapshot.Resolve(*outer).empty());
	snapshot.AddPlan(*connection.context, *outer);
	REQUIRE(snapshot.Resolve(*outer).empty());
	REQUIRE(snapshot.Resolve(captured).begin()->second.direct.source_cardinality == 100);
	REQUIRE(*calls == 1);

	auto scan = CreateCardinalityProvider(200, 200);
	scan->table_index = TableIndex(50);
	auto appended_calls = scan->bind_data->Cast<CardinalityProviderData>().calls;
	auto appended = LogicalProjection::CreateIdentity(TableIndex(60), std::move(scan));
	snapshot.AddPlan(*connection.context, *appended);
	REQUIRE(snapshot.Resolve(*appended).begin()->second.direct.source_cardinality == 200);
	REQUIRE(snapshot.Resolve(captured).begin()->second.direct.source_cardinality == 100);
	REQUIRE(*appended_calls == 1);
	REQUIRE(*calls == 1);
}

TEST_CASE("Nested read-only join regions share source provider calls", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto plan = unique_ptr<LogicalOperator>(CreateCardinalityProvider(100, 100));
	vector<shared_ptr<idx_t>> calls, column_calls;
	for (idx_t i = 0; i < 8; i++) {
		auto scan = CreateCardinalityProvider(100, 100);
		scan->table_index = TableIndex(20 + i);
		auto &data = scan->bind_data->Cast<CardinalityProviderData>();
		calls.push_back(data.calls);
		column_calls.push_back(data.column_calls);
		scan->function.statistics = [](ClientContext &, auto bind_data, column_t) -> unique_ptr<BaseStatistics> {
			auto &data = bind_data->template Cast<CardinalityProviderData>();
			(*data.column_calls)++;
			auto result = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
			result.SetDistinctCount(10);
			return result.ToUnique();
		};
		auto binding = plan->GetColumnBindings()[0];
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->conditions.emplace_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
		                              make_uniq<BoundColumnRefExpression>(
		                                  LogicalType::INTEGER, ColumnBinding(scan->table_index, ProjectionIndex(0))),
		                              ExpressionType::COMPARE_EQUAL);
		join->children.push_back(std::move(plan));
		join->children.push_back(std::move(scan));
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding));
		auto projection = make_uniq<LogicalProjection>(TableIndex(40 + i), std::move(expressions));
		projection->children.push_back(std::move(join));
		plan = std::move(projection);
	}
	RelationStatsExtractor extractor(*connection.context);
	REQUIRE(extractor.Extract(*plan));
	for (idx_t i = 0; i < calls.size(); i++) {
		REQUIRE(*calls[i] == 1);
		REQUIRE(*column_calls[i] == 1);
	}
	REQUIRE(extractor.Extract(*plan));
	for (auto &count : column_calls) {
		REQUIRE(*count == 1);
	}
}

TEST_CASE("Join-order UNION grouping reuses the original source snapshot", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto left = CreateCardinalityProvider(100, 100);
	auto right = CreateCardinalityProvider(100, 100);
	right->table_index = TableIndex(11);
	vector<shared_ptr<idx_t>> cardinality_calls, column_calls;
	vector<reference<LogicalGet>> scans {*left, *right};
	for (auto &scan : scans) {
		auto &data = scan.get().bind_data->Cast<CardinalityProviderData>();
		cardinality_calls.push_back(data.calls);
		column_calls.push_back(data.column_calls);
		scan.get().function.statistics = [](ClientContext &, auto bind_data, column_t) -> unique_ptr<BaseStatistics> {
			auto &data = bind_data->template Cast<CardinalityProviderData>();
			(*data.column_calls)++;
			auto result = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
			result.SetDistinctCount(*data.column_calls == 1 ? 10 : 1);
			return result.ToUnique();
		};
	}
	auto input = make_uniq<LogicalSetOperation>(TableIndex(30), 1, std::move(left), std::move(right),
	                                            LogicalOperatorType::LOGICAL_UNION, true);
	auto aggregate = make_uniq<LogicalAggregate>(TableIndex(40), TableIndex(50), vector<unique_ptr<Expression>> {});
	aggregate->groups.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(30), ProjectionIndex(0))));
	aggregate->grouping_sets.push_back({ProjectionIndex(0)});
	aggregate->children.push_back(std::move(input));
	JoinOrderOptimizer optimizer(*connection.context);
	RelationStats stats;
	auto plan = optimizer.Optimize(std::move(aggregate), stats);
	REQUIRE(stats.stats_initialized);
	REQUIRE(stats.cardinality == 20);
	for (idx_t i = 0; i < cardinality_calls.size(); i++) {
		REQUIRE(*cardinality_calls[i] == 1);
		REQUIRE(*column_calls[i] == 1);
	}
}

TEST_CASE("UNION grouping estimates ignore operator arm cardinalities", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto input = make_uniq<LogicalSetOperation>(TableIndex(30), 1, make_uniq<LogicalDummyScan>(TableIndex(10)),
	                                            make_uniq<LogicalDummyScan>(TableIndex(20)),
	                                            LogicalOperatorType::LOGICAL_UNION, true);
	LogicalAggregate aggregate(TableIndex(40), TableIndex(50), {});
	aggregate.groups.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(30), ProjectionIndex(0))));
	aggregate.grouping_sets.push_back({ProjectionIndex(0)});
	aggregate.children.push_back(std::move(input));

	auto bindings = aggregate.GetColumnBindings();
	for (auto operator_cardinality : {idx_t(0), idx_t(1000)}) {
		for (auto &arm : aggregate.children[0]->children) {
			arm->SetEstimatedCardinality(operator_cardinality);
		}
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(aggregate);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == 2);
		REQUIRE(stats->MatchesBindings(bindings));
		REQUIRE(extractor.ExtractedOperatorCount() == 4);
		for (auto &arm : aggregate.children[0]->children) {
			REQUIRE(arm->has_estimated_cardinality);
			REQUIRE(arm->estimated_cardinality == operator_cardinality);
		}
		REQUIRE_FALSE(aggregate.has_estimated_cardinality);
		REQUIRE_FALSE(aggregate.children[0]->has_estimated_cardinality);
	}
}

TEST_CASE("UNION grouping refinement shares the CTE extraction snapshot", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	vector<vector<unique_ptr<Expression>>> rows;
	for (idx_t row = 0; row < 3; row++) {
		vector<unique_ptr<Expression>> values;
		values.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
		rows.push_back(std::move(values));
	}
	auto input =
	    make_uniq<LogicalExpressionGet>(TableIndex(10), vector<LogicalType> {LogicalType::INTEGER}, std::move(rows));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(10)));
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(20)));
	auto definition = make_uniq<LogicalProjection>(TableIndex(20), std::move(expressions));
	definition->children.push_back(std::move(input));

	auto cte_index = TableIndex(30);
	auto first = make_uniq<LogicalCTERef>(TableIndex(40), cte_index,
	                                      vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
	                                      vector<Identifier> {"a", "b"});
	auto second = make_uniq<LogicalCTERef>(TableIndex(50), cte_index,
	                                       vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
	                                       vector<Identifier> {"c", "d"});
	auto union_op = make_uniq<LogicalSetOperation>(TableIndex(60), 2, std::move(first), std::move(second),
	                                               LogicalOperatorType::LOGICAL_UNION, true);
	LogicalAggregate aggregate(TableIndex(70), TableIndex(80), {});
	for (idx_t column = 0; column < 2; column++) {
		aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(
		    LogicalType::INTEGER, ColumnBinding(TableIndex(60), ProjectionIndex(column))));
	}
	aggregate.grouping_sets.push_back({ProjectionIndex(0), ProjectionIndex(1)});
	aggregate.children.push_back(std::move(union_op));

	idx_t cte_lookups = 0;
	RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		REQUIRE(index == cte_index);
		cte_lookups++;
		return *definition;
	});
	auto stats = extractor.Extract(aggregate);
	REQUIRE(stats);
	// Each arm has three rows but only one pair of keys, rather than the UNION's four domain combinations.
	REQUIRE(stats->cardinality == 2);
	REQUIRE(stats->MatchesBindings(aggregate.GetColumnBindings()));
	REQUIRE(cte_lookups == 4);
	REQUIRE(extractor.ExtractedOperatorCount() == 6);
	REQUIRE(extractor.Extract(aggregate) == stats);
	REQUIRE(cte_lookups == 4);
	REQUIRE(extractor.ExtractedOperatorCount() == 6);
}

TEST_CASE("UNION group refinement bounds shared CTE expansion", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	idx_t table_index = 100;
	auto constant_arm = [&]() {
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(10)));
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(20)));
		auto result = make_uniq<LogicalProjection>(TableIndex(table_index++), std::move(expressions));
		result->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(table_index++)));
		return result;
	};
	auto cte_ref = [&](idx_t definition_index) {
		return make_uniq<LogicalCTERef>(TableIndex(table_index++), TableIndex(definition_index),
		                                vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
		                                vector<Identifier> {"a", "b"});
	};
	vector<unique_ptr<LogicalOperator>> definitions;
	auto first = constant_arm();
	auto second = constant_arm();
	definitions.push_back(make_uniq<LogicalSetOperation>(TableIndex(table_index++), 2, std::move(first),
	                                                     std::move(second), LogicalOperatorType::LOGICAL_UNION, true));
	for (idx_t depth = 1; depth <= 12; depth++) {
		auto left = cte_ref(depth - 1);
		auto right = cte_ref(depth - 1);
		definitions.push_back(make_uniq<LogicalSetOperation>(
		    TableIndex(table_index++), 2, std::move(left), std::move(right), LogicalOperatorType::LOGICAL_UNION, true));
	}
	for (auto depth : {idx_t(4), idx_t(12)}) {
		CAPTURE(depth);
		auto input = cte_ref(depth);
		const auto expected = idx_t(1) << (depth + 1);
		auto stats = CreateStats(input->GetColumnBindings(), {3 * expected, 3 * expected}, 3 * expected);
		idx_t lookups = 0;
		RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
			lookups++;
			REQUIRE(index.index < definitions.size());
			return *definitions[index.index];
		});
		auto estimate = RelationStatisticsHelper::EstimateGroupCardinality(*input, stats, {0, 1}, extractor);
		// Exhausting the budget discards the partial sum and uses the ordinary estimate.
		REQUIRE(estimate == (depth == 4 ? expected : stats.cardinality));
		REQUIRE(lookups <= 1024);
		// Source extraction stays in the four shared leaf operators.
		REQUIRE(extractor.ExtractedOperatorCount() == 4);
	}
}

TEST_CASE("UNION group refinement bounds projection and CTE reference chains", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	vector<unique_ptr<LogicalOperator>> definitions;
	definitions.push_back(make_uniq<LogicalSetOperation>(TableIndex(10), 1, make_uniq<LogicalDummyScan>(TableIndex(11)),
	                                                     make_uniq<LogicalDummyScan>(TableIndex(12)),
	                                                     LogicalOperatorType::LOGICAL_UNION, true));
	for (idx_t i = 1; i <= 600; i++) {
		auto ref_index = TableIndex(100 + 2 * i);
		auto ref = make_uniq<LogicalCTERef>(ref_index, TableIndex(i - 1), vector<LogicalType> {LogicalType::INTEGER},
		                                    vector<Identifier> {"k"});
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(ref_index, ProjectionIndex(0))));
		auto projection = make_uniq<LogicalProjection>(TableIndex(101 + 2 * i), std::move(expressions));
		projection->children.push_back(std::move(ref));
		definitions.push_back(std::move(projection));
	}
	for (auto depth : {idx_t(4), idx_t(600)}) {
		CAPTURE(depth);
		auto &input = *definitions[depth];
		auto stats = CreateStats(input.GetColumnBindings(), {100}, 100);
		idx_t lookups = 0;
		RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
			lookups++;
			return *definitions[index.index];
		});
		REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(input, stats, {0}, extractor) ==
		        (depth == 4 ? 2 : 100));
		REQUIRE(lookups <= 512);
	}
}

TEST_CASE("UNION group refinement rejects unresolved and cyclic CTE layouts", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	LogicalCTERef input(TableIndex(10), TableIndex(20), {LogicalType::INTEGER, LogicalType::INTEGER}, {"a", "b"});
	LogicalDummyScan wrong_width(TableIndex(30));
	auto stats = CreateStats(input.GetColumnBindings(), {100, 100}, 1000);
	auto fallback = RelationStatisticsHelper::EstimateDistinctCardinality(
	    {DistinctCount(100, DistinctCountSource::EXACT), DistinctCount(100, DistinctCountSource::EXACT)}, 1000);
	RelationStatsExtractor no_callback(*connection.context);
	REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(input, stats, {0, 1}, no_callback) == fallback);
	for (idx_t mode = 0; mode < 3; mode++) {
		CAPTURE(mode);
		RelationStatsExtractor extractor(*connection.context, [&](TableIndex) -> optional_ptr<LogicalOperator> {
			if (mode == 0) {
				return nullptr;
			}
			if (mode == 1) {
				return wrong_width;
			}
			return input;
		});
		REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(input, stats, {0, 1}, extractor) == fallback);
	}
	input.is_recurring = true;
	idx_t lookups = 0;
	RelationStatsExtractor recurring(*connection.context, [&](TableIndex) -> optional_ptr<LogicalOperator> {
		lookups++;
		return wrong_width;
	});
	REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(input, stats, {0, 1}, recurring) == fallback);
	REQUIRE(lookups == 0);
}

TEST_CASE("Shared CTE grouping estimates distinguish requested output positions", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	vector<vector<unique_ptr<Expression>>> rows;
	for (idx_t i = 1; i <= 3; i++) {
		vector<unique_ptr<Expression>> row;
		row.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(i)));
		rows.push_back(std::move(row));
	}
	auto values =
	    make_uniq<LogicalExpressionGet>(TableIndex(10), vector<LogicalType> {LogicalType::INTEGER}, std::move(rows));
	vector<unique_ptr<Expression>> columns;
	columns.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(10), ProjectionIndex(0))));
	columns.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(0)));
	auto definition = make_uniq<LogicalProjection>(TableIndex(20), std::move(columns));
	definition->children.push_back(std::move(values));
	auto arm = [&](idx_t column) {
		auto ref_index = TableIndex(40 + column);
		auto ref = make_uniq<LogicalCTERef>(ref_index, TableIndex(30),
		                                    vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
		                                    vector<Identifier> {"varying", "constant"});
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
		                                                          ColumnBinding(ref_index, ProjectionIndex(column))));
		auto projection = make_uniq<LogicalProjection>(TableIndex(50 + column), std::move(expressions));
		projection->children.push_back(std::move(ref));
		return projection;
	};
	LogicalSetOperation input(TableIndex(60), 1, arm(0), arm(1), LogicalOperatorType::LOGICAL_UNION, true);
	auto stats = CreateStats(input.GetColumnBindings(), {6}, 6);
	RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		REQUIRE(index == TableIndex(30));
		return *definition;
	});
	// The first arm contributes three keys, the second contributes one.
	REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(input, stats, {0}, extractor) == 4);
}

TEST_CASE("Group estimates without a UNION do not extract their input again", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	auto input = CreateProjectedDummy(TableIndex(10), TableIndex(20));
	auto stats = CreateStats(input->GetColumnBindings(), {10}, 100);
	RelationStatsExtractor extractor(*connection.context);
	REQUIRE(RelationStatisticsHelper::EstimateGroupCardinality(*input, stats, {0}, extractor) == 10);
	REQUIRE(extractor.ExtractedOperatorCount() == 0);
}

TEST_CASE("Standalone join statistics share estimation without mutating the plan", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	for (auto query : {"SELECT a.x,b.y FROM stats_a a JOIN stats_b b USING(k)",
	                   "SELECT a.x,b.y FROM stats_a a JOIN stats_b b ON a.k<b.k",
	                   "SELECT a.x,b.y FROM stats_a a JOIN stats_b b ON a.k<=b.k",
	                   "SELECT a.x,b.y FROM stats_a a,stats_b b WHERE a.k=b.k",
	                   "SELECT a.x,b.y,c.z FROM stats_a a JOIN stats_b b USING(k) JOIN stats_c c USING(k)",
	                   "SELECT a.x,b.y,c.z FROM stats_a a,stats_b b,stats_c c WHERE a.k=b.k AND b.k=c.k AND a.k=c.k",
	                   "SELECT a.x,b.y FROM (SELECT * FROM stats_a WHERE x<100) a JOIN stats_b b USING(k)",
	                   "SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b USING(k)",
	                   "SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b ON b.k=a.k",
	                   "SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b ON a.k=b.k AND a.x<b.y",
	                   "SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b USING(k) WHERE b.y IS NULL",
	                   "SELECT a.x,b.y FROM (SELECT * FROM stats_a WHERE x<100) a LEFT JOIN stats_b b USING(k)",
	                   "SELECT a.x,q.y FROM stats_a a LEFT JOIN "
	                   "(SELECT b.k,b.y FROM stats_b b JOIN stats_c c USING(k)) q USING(k)",
	                   "SELECT a.x,q.y FROM stats_a a LEFT JOIN "
	                   "(SELECT b.k,b.y FROM stats_b b LEFT JOIN stats_c c USING(k)) q USING(k)",
	                   "SELECT a.x,b.y,c.z FROM stats_a a JOIN stats_b b USING(k) LEFT JOIN stats_c c USING(k)",
	                   "SELECT x,y,count(*) FROM (SELECT a.x,b.y FROM stats_a a JOIN stats_b b USING(k) UNION ALL "
	                   "SELECT a.x,b.y FROM stats_a a JOIN stats_b b USING(k)) u GROUP BY x,y",
	                   "SELECT x,y,count(*) FROM (SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b USING(k) UNION ALL "
	                   "SELECT a.x,b.y FROM stats_a a LEFT JOIN stats_b b USING(k)) u GROUP BY x,y"}) {
		CAPTURE(query);
		auto plan = PlanStatisticsQuery(connection, query);
		auto reference_plan = PlanStatisticsQuery(connection, query);
		RelationStats reference_stats;
		JoinOrderOptimizer reference_optimizer(*connection.context);
		reference_plan = reference_optimizer.Optimize(std::move(reference_plan), reference_stats);
		REQUIRE(reference_stats.stats_initialized);

		SetStatisticsTestCardinalities(*plan);
		auto bindings = plan->GetColumnBindings();
		MemoryStream before;
		BinarySerializer::Serialize(*plan, before);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*plan);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == reference_stats.cardinality);
		REQUIRE(stats->max_cardinality == reference_stats.max_cardinality);
		REQUIRE(stats->row_retention == reference_stats.row_retention);
		REQUIRE(stats->cardinality > 10000);
		REQUIRE(stats->MatchesBindings(bindings));
		REQUIRE(stats->columns.size() == reference_stats.columns.size());
		for (idx_t i = 0; i < stats->columns.size(); i++) {
			REQUIRE(stats->columns[i].current_domain.distinct_count ==
			        reference_stats.columns[i].current_domain.distinct_count);
		}
		MemoryStream after;
		BinarySerializer::Serialize(*plan, after);
		REQUIRE(before.GetPosition() == after.GetPosition());
		REQUIRE(memcmp(before.GetData(), after.GetData(), before.GetPosition()) == 0);
	}
	connection.Rollback();
}

TEST_CASE("Standalone LEFT preparation retains one complete right input", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	for (auto rhs : {
	         "stats_b b JOIN stats_c c ON b.k=c.k",
	         "stats_b b LEFT JOIN stats_c c ON b.k=c.k",
	         "(SELECT * FROM stats_b WHERE y<100) b JOIN stats_c c ON b.k=c.k",
	     }) {
		CAPTURE(rhs);
		auto plan =
		    PlanStatisticsQuery(connection, string("SELECT a.x,b.y FROM stats_a a LEFT JOIN (") + rhs + ") ON a.k=b.k");
		auto &join = plan->children[0]->Cast<LogicalComparisonJoin>();
		RelationStatsExtractor extractor(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE(graph.BuildForEstimation(extractor, join));
		REQUIRE(graph.relation_manager.NumRelations() == 2);
		auto inputs = graph.relation_manager.GetRelationStats();
		auto &right = inputs[1];
		REQUIRE(right.MatchesBindings(join.children[1]->GetColumnBindings()));
		for (auto &op : graph.GetJoinOperators()) {
			if (op->type == JoinOrderOperatorType::LEFT) {
				REQUIRE(op->right_relations.get().count == 1);
				REQUIRE(op->must_apply_as_operator);
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Standalone LEFT preparation declines unsupported boundary predicates", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (idx_t mode = 0; mode < 5; mode++) {
		CAPTURE(mode);
		LogicalComparisonJoin join(JoinType::LEFT);
		join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(10)));
		join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(20)));
		auto left = make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
		                                                ColumnBinding(TableIndex(10), ProjectionIndex(0)));
		auto right = make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
		                                                 ColumnBinding(TableIndex(20), ProjectionIndex(0)));
		if (mode == 0) {
			join.conditions.emplace_back(left->Copy(), make_uniq<BoundConstantExpression>(Value::INTEGER(1)),
			                             ExpressionType::COMPARE_EQUAL);
			join.conditions.emplace_back(right->Copy(), make_uniq<BoundConstantExpression>(Value::INTEGER(1)),
			                             ExpressionType::COMPARE_EQUAL);
		} else if (mode == 1 || mode == 2) {
			auto comparison =
			    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(left), std::move(right));
			if (mode == 1) {
				// A mixed-input operand cannot be normalized to one semantic side.
				join.conditions.emplace_back(std::move(comparison),
				                             make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)),
				                             ExpressionType::COMPARE_EQUAL);
			} else {
				join.conditions.emplace_back(std::move(comparison));
			}
		} else {
			join.conditions.emplace_back(
			    make_uniq<BoundConstantExpression>(mode == 3 ? Value::BOOLEAN(false) : Value(LogicalType::BOOLEAN)));
		}
		RelationStatsExtractor extractor(*connection.context);
		QueryGraphManager graph(*connection.context);
		REQUIRE_FALSE(graph.BuildForEstimation(extractor, join));
		auto stats = extractor.Extract(join);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == 1);
		REQUIRE(stats->MatchesBindings(join.GetColumnBindings()));
	}
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	auto plan =
	    PlanStatisticsQuery(connection, "SELECT * FROM stats_a a LEFT JOIN stats_b b ON a.k=b.k AND random()>0.1");
	RelationStatsExtractor extractor(*connection.context);
	QueryGraphManager graph(*connection.context);
	REQUIRE_FALSE(graph.BuildForEstimation(extractor, *plan->children[0]));
	REQUIRE(extractor.Extract(*plan));
	connection.Rollback();
}

TEST_CASE("Standalone and optimizing join estimates use the same source provenance",
          "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	REQUIRE_NO_FAIL(connection.Query("ANALYZE"));
	connection.BeginTransaction();
	for (auto query : {
	         "SELECT a.x FROM stats_a a JOIN range(500) d(k) USING(k)",
	         "SELECT a.x FROM (SELECT x,k FROM stats_a) a JOIN range(500) d(k) USING(k)",
	         "SELECT a.x FROM (SELECT a.x,a.k,a.x%2 AS g FROM stats_a a JOIN stats_c c USING(k)) a "
	         "JOIN range(500) d(k) USING(k)",
	         "SELECT a.x FROM stats_a a JOIN (SELECT range AS k FROM range(500) LIMIT 10) d USING(k)",
	     }) {
		CAPTURE(query);
		auto plan = PlanStatisticsQuery(connection, query);
		auto optimized = PlanStatisticsQuery(connection, query);
		RelationStats reference;
		JoinOrderOptimizer optimizer(*connection.context);
		optimized = optimizer.Optimize(std::move(optimized), reference);
		REQUIRE(reference.stats_initialized);
		SetStatisticsTestCardinalities(*plan);
		MemoryStream before;
		BinarySerializer::Serialize(*plan, before);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*plan);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == reference.cardinality);
		MemoryStream after;
		BinarySerializer::Serialize(*plan, after);
		REQUIRE(before.GetPosition() == after.GetPosition());
		REQUIRE(memcmp(before.GetData(), after.GetData(), before.GetPosition()) == 0);
	}
	connection.Rollback();
}

TEST_CASE("Standalone join estimates preserve positive fractional cardinalities", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	vector<pair<string, double>> queries {{"SELECT * FROM range(1) a(i) JOIN range(1) b(i) ON a.i<b.i", 0.5},
	                                      {"SELECT * FROM (SELECT * FROM range(1000) LIMIT 1) a "
	                                       "JOIN (SELECT * FROM range(1000) LIMIT 1) b USING(range)",
	                                       0.001},
	                                      {"SELECT a.i,b.i FROM "
	                                       "(SELECT a.i FROM range(1) a(i) JOIN range(1) b(i) ON a.i<b.i) a "
	                                       "LEFT JOIN (SELECT 0::BIGINT i FROM range(3)) b USING(i)",
	                                       1.5}};
	for (auto &entry : queries) {
		auto &query = entry.first;
		CAPTURE(query);
		auto plan = PlanStatisticsQuery(connection, query);
		auto reference_plan = PlanStatisticsQuery(connection, query);
		RelationStats reference_stats;
		JoinOrderOptimizer optimizer(*connection.context);
		reference_plan = optimizer.Optimize(std::move(reference_plan), reference_stats);
		REQUIRE(reference_stats.stats_initialized);
		REQUIRE(reference_stats.cardinality.GetDouble() == Approx(entry.second));
		REQUIRE(reference_stats.GetCardinalityForPlan() == 1);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*plan);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == reference_stats.cardinality);
	}
	connection.Rollback();
}

TEST_CASE("Standalone join estimates preserve empty inputs", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	vector<pair<string, idx_t>> queries {
	    {"SELECT a.x,b.y FROM (SELECT * FROM stats_a LIMIT 0) a CROSS JOIN stats_b b", 0},
	    {"SELECT a.x,b.y FROM stats_a a CROSS JOIN (SELECT * FROM stats_b LIMIT 0) b", 0},
	    {"SELECT a.x,b.y FROM (SELECT * FROM stats_a LIMIT 0) a JOIN stats_b b USING(k)", 0},
	    {"SELECT a.x,b.y FROM (SELECT * FROM stats_a LIMIT 0) a LEFT JOIN stats_b b USING(k)", 0},
	    {"SELECT a.x,b.y FROM stats_a a LEFT JOIN (SELECT * FROM stats_b LIMIT 0) b USING(k)", 1000},
	    {"SELECT b.y FROM (SELECT * FROM stats_a LIMIT 0) a CROSS JOIN stats_b b GROUP BY b.y", 0},
	    {"SELECT a.x FROM stats_a a SEMI JOIN "
	     "(SELECT b.k FROM (SELECT * FROM stats_c LIMIT 0) c CROSS JOIN stats_b b GROUP BY b.k) q USING(k)",
	     0},
	    {"SELECT a.x FROM stats_a a ANTI JOIN "
	     "(SELECT b.k FROM (SELECT * FROM stats_c LIMIT 0) c CROSS JOIN stats_b b GROUP BY b.k) q USING(k)",
	     1000}};
	for (bool empty_first : {false, true}) {
		string query = "SELECT 1 FROM ";
		const string empty = "(SELECT * FROM range(1) LIMIT 0) e";
		if (empty_first) {
			query += empty + " CROSS JOIN ";
		}
		for (idx_t i = 0; i < 18; i++) {
			if (i > 0) {
				query += " CROSS JOIN ";
			}
			query += "range(1000000000000000000) r" + to_string(i);
		}
		if (!empty_first) {
			query += " CROSS JOIN " + empty;
		}
		queries.emplace_back(std::move(query), 0);
	}
	for (auto &query : queries) {
		CAPTURE(query.first);
		auto plan = PlanStatisticsQuery(connection, query.first);
		SetStatisticsTestCardinalities(*plan);
		MemoryStream before;
		BinarySerializer::Serialize(*plan, before);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*plan);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == query.second);
		MemoryStream after;
		BinarySerializer::Serialize(*plan, after);
		REQUIRE(before.GetPosition() == after.GetPosition());
		REQUIRE(memcmp(before.GetData(), after.GetData(), before.GetPosition()) == 0);
	}
	connection.Rollback();
}

TEST_CASE("Standalone join preparation declines unavailable predicate bindings", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	for (auto depth : {idx_t(0), idx_t(1)}) {
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(10)));
		join->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(20)));
		auto left_binding = ColumnBinding(TableIndex(depth == 0 ? 30 : 10), ProjectionIndex(0));
		join->conditions.emplace_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, left_binding, depth),
		                              make_uniq<BoundColumnRefExpression>(
		                                  LogicalType::INTEGER, ColumnBinding(TableIndex(20), ProjectionIndex(0))),
		                              ExpressionType::COMPARE_EQUAL);
		RelationStatsExtractor extractor(*connection.context);
		auto stats = extractor.Extract(*join);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == 1);
		REQUIRE(stats->MatchesBindings(join->GetColumnBindings()));
		auto projection = LogicalProjection::CreateIdentity(TableIndex(40), std::move(join));
		RelationStatsExtractor projected_extractor(*connection.context);
		auto projected_stats = projected_extractor.Extract(*projection);
		REQUIRE(projected_stats);
		REQUIRE(projected_stats->cardinality == 1);
		REQUIRE(projected_stats->MatchesBindings(projection->GetColumnBindings()));
	}
}

TEST_CASE("Repeated relation outputs share a normalized binding", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	LogicalComparisonJoin join(JoinType::SINGLE);
	join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(10)));
	join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(20)));
	join.left_projection_map = {ProjectionIndex(0), ProjectionIndex(0)};
	join.ResolveOperatorTypes();
	auto bindings = join.GetColumnBindings();
	REQUIRE(bindings.size() == 3);
	REQUIRE(bindings[0] == bindings[1]);

	RelationStatsExtractor extractor(*connection.context);
	auto stats = extractor.Extract(join);
	REQUIRE(stats);
	REQUIRE(stats->MatchesBindings(bindings));
	RelationManager normal(*connection.context);
	REQUIRE(normal.AddRelation(join, nullptr, *stats));
	RelationManager standalone(*connection.context);
	vector<reference<LogicalOperator>> filters;
	REQUIRE(standalone.ExtractJoinRelationsForEstimation(extractor, join, filters));
	for (auto &manager : {reference<RelationManager>(normal), reference<RelationManager>(standalone)}) {
		ColumnBinding normalized;
		REQUIRE(manager.get().TryNormalizeBinding(bindings[0], normalized));
		REQUIRE(normalized == ColumnBinding(TableIndex(0), ProjectionIndex(0)));
		REQUIRE(manager.get().TryNormalizeBinding(bindings[2], normalized));
		REQUIRE(normalized == ColumnBinding(TableIndex(0), ProjectionIndex(2)));
		REQUIRE(manager.get().GetRelationStats()[0].MatchesBindings(bindings));
	}
}

TEST_CASE("Standalone joins inside CTE definitions retain the caller snapshot", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	auto definition = PlanStatisticsQuery(connection, "SELECT a.x,b.y FROM stats_a a JOIN stats_b b USING(k)");
	SetStatisticsTestCardinalities(*definition);
	auto cte_index = TableIndex(10000);
	auto first = make_uniq<LogicalCTERef>(TableIndex(10001), cte_index,
	                                      vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
	                                      vector<Identifier> {"x", "y"});
	auto second = make_uniq<LogicalCTERef>(TableIndex(10002), cte_index,
	                                       vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER},
	                                       vector<Identifier> {"x", "y"});
	LogicalSetOperation union_op(TableIndex(10003), 2, std::move(first), std::move(second),
	                             LogicalOperatorType::LOGICAL_UNION, true);
	idx_t lookups = 0;
	RelationStatsExtractor extractor(*connection.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		REQUIRE(index == cte_index);
		lookups++;
		return *definition;
	});
	MemoryStream before;
	BinarySerializer::Serialize(*definition, before);
	auto stats = extractor.Extract(union_op);
	REQUIRE(stats);
	REQUIRE(stats->cardinality > 100000);
	auto definition_stats = extractor.Extract(*definition);
	REQUIRE(definition_stats);
	REQUIRE(stats->cardinality == 2 * definition_stats->cardinality);
	REQUIRE(lookups == 2);
	MemoryStream after;
	BinarySerializer::Serialize(*definition, after);
	REQUIRE(before.GetPosition() == after.GetPosition());
	REQUIRE(memcmp(before.GetData(), after.GetData(), before.GetPosition()) == 0);
	connection.Rollback();
}

TEST_CASE("Standalone joins retain projected and repeated output bindings", "[optimizer][relation_statistics]") {
	DuckDB db;
	Connection connection(db);
	CreateStatisticsJoinTables(connection);
	connection.BeginTransaction();
	for (auto type : {"JOIN", "LEFT JOIN"}) {
		CAPTURE(type);
		auto plan =
		    PlanStatisticsQuery(connection, string("SELECT * FROM stats_a a ") + type + " stats_b b ON a.k=b.k");
		auto &join = plan->children[0]->Cast<LogicalComparisonJoin>();
		RelationStatsExtractor original(*connection.context);
		auto full = original.Extract(join);
		REQUIRE(full);
		join.left_projection_map = {ProjectionIndex(0)};
		join.right_projection_map = {ProjectionIndex(0), ProjectionIndex(0)};
		join.ResolveOperatorTypes();
		SetStatisticsTestCardinalities(join);
		MemoryStream before;
		BinarySerializer::Serialize(join, before);
		RelationStatsExtractor projected(*connection.context);
		auto stats = projected.Extract(join);
		REQUIRE(stats);
		REQUIRE(stats->cardinality == full->cardinality);
		REQUIRE(stats->MatchesBindings(join.GetColumnBindings()));
		REQUIRE(stats->columns.size() == 3);
		REQUIRE(stats->columns[1].binding == stats->columns[2].binding);
		for (auto &column : stats->columns) {
			auto source = full->GetColumnStats(column.binding);
			REQUIRE(source);
			REQUIRE(column.current_domain.distinct_count == source->current_domain.distinct_count);
		}
		MemoryStream after;
		BinarySerializer::Serialize(join, after);
		REQUIRE(before.GetPosition() == after.GetPosition());
		REQUIRE(memcmp(before.GetData(), after.GetData(), before.GetPosition()) == 0);
	}
	connection.Rollback();
}

static unique_ptr<BoundAggregateExpression> BindDomainAggregate(ClientContext &context, const char *name,
                                                                unique_ptr<Expression> argument) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), name));
	auto function = entry.functions.GetFunctionByArguments(context, {argument->GetReturnType()});
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(argument));
	return FunctionBinder(context).BindAggregateFunction(function, std::move(children));
}

TEST_CASE("Singleton groups preserve the input current domain for first",
          "[optimizer][relation_statistics][selected_value_statistics]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)"));
	connection.BeginTransaction();
	auto input = PlanStatisticsQuery(connection, "SELECT id, x FROM t");
	auto bindings = input->GetColumnBindings();
	auto stats = CreateStats(bindings, {64, 8}, 64);

	// Both aggregates select x from the same singleton groups. FILTER makes only
	// the second aggregate take the weaker selection rule.
	auto first = BindDomainAggregate(*connection.context, "first",
	                                 make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, bindings[1]));
	auto filtered = first->Copy();
	filtered->Cast<BoundAggregateExpression>().GetFilterMutable() =
	    make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(first));
	expressions.push_back(std::move(filtered));
	LogicalAggregate aggregate(TableIndex(20), TableIndex(30), std::move(expressions));
	aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, bindings[0]));
	aggregate.children.push_back(std::move(input));

	auto result = RelationStatisticsHelper::ExtractAggregationStats(aggregate, stats, *connection.context);
	REQUIRE(result);
	REQUIRE(result->columns.size() == 3);
	REQUIRE(result->columns[1].current_domain.source == DistinctCountSource::EXACT);
	REQUIRE(result->columns[1].current_domain.distinct_count == 8);
	REQUIRE(result->columns[2].total_domain.source == DistinctCountSource::EXACT);
	REQUIRE(result->columns[2].current_domain.source == DistinctCountSource::CARDINALITY);
	connection.Rollback();
}

TEST_CASE("Selected aggregates preserve source domains without structural evidence",
          "[optimizer][relation_statistics][selected_value_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	auto group = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto value = ColumnBinding(TableIndex(10), ProjectionIndex(1));
	for (auto type : vector<LogicalType> {LogicalType::INTEGER, LogicalType::DECIMAL(12, 2), LogicalType::VARCHAR,
	                                      LogicalType::STRUCT({{"key", LogicalType::BIGINT}})}) {
		for (auto name : {"first", "last", "any_value", "arbitrary"}) {
			INFO(name << " " << type.ToString());
			auto expression =
			    BindDomainAggregate(*connection.context, name, make_uniq<BoundColumnRefExpression>(type, value));
			// Exercise the actual bound implementation after copying and rebinding,
			// including the DECIMAL specialization and the arbitrary alias.
			auto copy = expression->Copy();
			MemoryStream stream;
			BinarySerializer::Serialize(*copy, stream);
			stream.Rewind();
			bound_parameter_map_t parameters;
			auto restored = BinaryDeserializer::Deserialize<Expression>(stream, *connection.context, parameters);
			vector<unique_ptr<Expression>> expressions;
			expressions.push_back(std::move(copy));
			expressions.push_back(std::move(restored));
			LogicalAggregate aggregate(TableIndex(20), TableIndex(30), std::move(expressions));
			aggregate.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, group));
			auto child = CreateStats({group, value}, {10, 100}, 100);
			child.columns[1].total_domain = DistinctCount(1000, DistinctCountSource::HLL);
			child.columns[1].semi_anti_join_domain_evidence.MarkUnique();
			child.columns[1].semi_anti_join_domain_evidence.TightenDomainBound(100);
			auto result = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child, *connection.context);
			REQUIRE(result);
			REQUIRE(result->columns.size() == 3);
			for (idx_t i = 1; i < result->columns.size(); i++) {
				const auto &selected = result->columns[i];
				REQUIRE(selected.total_domain.distinct_count == 1000);
				REQUIRE(selected.total_domain.source == DistinctCountSource::HLL);
				REQUIRE(selected.current_domain.distinct_count == MinValue(CardinalityValue(100), result->cardinality));
				REQUIRE(selected.current_domain.source == DistinctCountSource::CARDINALITY);
				REQUIRE_FALSE(selected.GetSupportedSemiAntiDomainSize());
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Selected aggregate domain propagation falls back without a proven source",
          "[optimizer][relation_statistics][selected_value_statistics]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	auto value = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	for (const auto &mode : {"plain", "sum", "constant", "cast", "state", "replaced", "missing", "filter",
	                         "type_mismatch", "collation_mismatch"}) {
		INFO(mode);
		auto type = string(mode) == "collation_mismatch" ? LogicalType::VARCHAR : LogicalType::INTEGER;
		unique_ptr<Expression> argument = make_uniq<BoundColumnRefExpression>(type, value);
		if (string(mode) == "constant") {
			argument = make_uniq<BoundConstantExpression>(Value::INTEGER(3));
		} else if (string(mode) == "cast") {
			argument =
			    BoundCastExpression::AddCastToType(*connection.context, std::move(argument), LogicalType::BIGINT);
		}
		auto expression =
		    BindDomainAggregate(*connection.context, string(mode) == "sum" ? "sum" : "first", std::move(argument));
		if (string(mode) == "type_mismatch") {
			expression->SetReturnType(LogicalType::BIGINT);
		} else if (string(mode) == "collation_mismatch") {
			expression->SetReturnType(LogicalType::VARCHAR_COLLATION("nocase"));
		} else if (string(mode) == "state") {
			expression->StateExportModeMutable() = AggregateStateExportMode::STATE_EXPORT;
		} else if (string(mode) == "replaced") {
			// Keep the name and original catalog definition of first, but execute min.
			auto replacement = BindDomainAggregate(*connection.context, "min", expression->GetChildren()[0]->Copy());
			auto &function = expression->FunctionMutable();
			auto definition = function.GetDefinition();
			function.ReplaceImplementation(replacement->Function());
			function.SetName("first");
			REQUIRE(function.GetDefinition() == definition);
		} else if (string(mode) == "filter") {
			expression->GetFilterMutable() = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
		}
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(std::move(expression));
		LogicalAggregate aggregate(TableIndex(20), TableIndex(30), std::move(expressions));
		auto child = string(mode) == "missing" ? CreateStats({}, {}, 100) : CreateStats({value}, {40}, 100);
		auto result = RelationStatisticsHelper::ExtractAggregationStats(aggregate, child, *connection.context);
		REQUIRE(result);
		REQUIRE(result->cardinality == 1);
		const bool selected = string(mode) == "plain" || string(mode) == "filter";
		REQUIRE(result->columns[0].total_domain.distinct_count == (selected ? 40 : 1));
		REQUIRE(result->columns[0].total_domain.source ==
		        (selected ? DistinctCountSource::EXACT : DistinctCountSource::CARDINALITY));
		REQUIRE(result->columns[0].current_domain.source == DistinctCountSource::CARDINALITY);
	}
	connection.Rollback();
}
