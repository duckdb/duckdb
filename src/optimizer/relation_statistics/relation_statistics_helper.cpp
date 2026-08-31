#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <math.h>

namespace duckdb {

struct ExpressionBinding {
	bool FoundExpression() const {
		return expression;
	}

	bool FoundColumnRef() const {
		return FoundExpression() && expression->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF;
	}

	optional_ptr<Expression> expression;
	ColumnBinding child_binding;
	bool expression_is_constant = false;
};

static ExpressionBinding GetChildColumnBinding(Expression &expr) {
	ExpressionBinding result;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.GetChildren().empty()) {
			result.expression = expr;
			result.expression_is_constant = true;
			return result;
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		result.expression = expr;
		result.child_binding = expr.Cast<BoundColumnRefExpression>().Binding();
		return result;
	}
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		result.expression = expr;
		result.expression_is_constant = true;
		return result;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		if (result.FoundColumnRef()) {
			return;
		}
		auto child_result = GetChildColumnBinding(*child);
		if (child_result.FoundExpression()) {
			result = child_result;
		}
	});
	return result;
}

optional<RelationStats> RelationStatisticsHelper::ExtractProjectionStats(LogicalProjection &projection,
                                                                         const RelationStats &child_stats) {
	RelationStats result;
	result.cardinality = child_stats.cardinality;
	result.table_name = Identifier(projection.GetName());
	result.stats_initialized = true;
	auto bindings = projection.GetColumnBindings();
	D_ASSERT(bindings.size() == projection.expressions.size());
	for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
		auto &expression = *projection.expressions[expression_idx];
		auto expression_binding = GetChildColumnBinding(expression);
		DistinctCount distinct_count(result.cardinality, DistinctCountSource::CARDINALITY);
		if (expression_binding.expression_is_constant) {
			distinct_count = DistinctCount(MinValue<idx_t>(result.cardinality, 1), DistinctCountSource::EXACT);
		} else if (expression_binding.FoundColumnRef()) {
			auto child_column = child_stats.GetColumnStats(expression_binding.child_binding);
			if (!child_column) {
				return {};
			}
			distinct_count = child_column->distinct_count;
		}
		result.columns.emplace_back(bindings[expression_idx], distinct_count, Identifier(expression.GetName()));
	}
	result.Verify(bindings);
	return result;
}

idx_t RelationStatisticsHelper::EstimateDistinctCardinality(const vector<DistinctCount> &distinct_counts,
                                                            idx_t input_cardinality) {
	if (distinct_counts.empty()) {
		return input_cardinality / 2;
	}
	double product = 1;
	for (auto &distinct_count : distinct_counts) {
		product *= static_cast<double>(MaxValue<idx_t>(distinct_count.distinct_count, 1));
	}
	product *= pow(0.95, static_cast<double>(distinct_counts.size() - 1));
	const auto multiplier = 1.0 - exp(-static_cast<double>(input_cardinality) / product);
	const auto estimate = multiplier == 0 ? static_cast<double>(input_cardinality) : product * multiplier;
	auto result = LossyNumericCast<idx_t>(MinValue(estimate, static_cast<double>(input_cardinality)));
	return input_cardinality > 0 ? MaxValue<idx_t>(result, 1) : 0;
}

optional<RelationStats> RelationStatisticsHelper::ExtractAggregationStats(LogicalAggregate &aggregate,
                                                                          const RelationStats &child_stats) {
	vector<DistinctCount> cardinality_counts;
	for (auto &grouping_set : aggregate.grouping_sets) {
		vector<DistinctCount> set_counts;
		for (auto group_idx : grouping_set) {
			auto &group = aggregate.GetGroupExpression(group_idx);
			if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto column = child_stats.GetColumnStats(group.Cast<BoundColumnRefExpression>().Binding());
			if (!column) {
				return {};
			}
			auto count = column->distinct_count;
			count.distinct_count = MaxValue<idx_t>(count.distinct_count, 1);
			set_counts.push_back(count);
		}
		if (set_counts.size() > cardinality_counts.size()) {
			cardinality_counts = std::move(set_counts);
		}
	}

	RelationStats result;
	result.cardinality =
	    aggregate.groups.empty() ? 1 : EstimateDistinctCardinality(cardinality_counts, child_stats.cardinality);
	result.table_name = Identifier(aggregate.GetName());
	result.stats_initialized = true;
	auto bindings = aggregate.GetColumnBindings();
	for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
		auto &group = *aggregate.groups[group_idx];
		DistinctCount distinct_count(result.cardinality, DistinctCountSource::CARDINALITY);
		if (group.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto child_column = child_stats.GetColumnStats(group.Cast<BoundColumnRefExpression>().Binding());
			if (!child_column) {
				return {};
			}
			distinct_count = child_column->distinct_count;
		}
		result.columns.emplace_back(bindings[result.columns.size()], distinct_count, Identifier(group.GetName()));
	}
	for (auto &expression : aggregate.expressions) {
		result.columns.emplace_back(bindings[result.columns.size()],
		                            DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier(expression->GetName()));
	}
	for (idx_t grouping_idx = 0; grouping_idx < aggregate.grouping_functions.size(); grouping_idx++) {
		auto grouping_count = MinValue<idx_t>(result.cardinality, MaxValue<idx_t>(aggregate.grouping_sets.size(), 1));
		result.columns.emplace_back(bindings[result.columns.size()],
		                            DistinctCount(grouping_count, DistinctCountSource::CARDINALITY),
		                            Identifier("grouping"));
	}
	result.Verify(bindings);
	return result;
}

optional<RelationStats> RelationStatisticsHelper::ExtractWindowStats(LogicalWindow &window,
                                                                     const RelationStats &child_stats) {
	RelationStats result;
	result.cardinality = child_stats.cardinality;
	result.table_name = Identifier(window.GetName());
	result.stats_initialized = true;
	for (auto &binding : window.GetColumnBindings()) {
		auto child_column = child_stats.GetColumnStats(binding);
		if (child_column) {
			result.columns.emplace_back(binding, child_column->distinct_count, child_column->name);
		} else if (binding.table_index == window.window_index) {
			result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
			                            Identifier("window"));
		} else {
			return {};
		}
	}
	result.Verify(window.GetColumnBindings());
	return result;
}

static optional<DistinctCount> GetDistinctTargetCount(Expression &target, const RelationStats &child_stats) {
	switch (target.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto column = child_stats.GetColumnStats(target.Cast<BoundColumnRefExpression>().Binding());
		return column ? optional<DistinctCount>(column->distinct_count) : optional<DistinctCount>();
	}
	case ExpressionClass::BOUND_REF: {
		auto index = target.Cast<BoundReferenceExpression>().Index();
		return index < child_stats.columns.size() ? optional<DistinctCount>(child_stats.columns[index].distinct_count)
		                                          : optional<DistinctCount>();
	}
	case ExpressionClass::BOUND_CONSTANT:
		return DistinctCount(MinValue<idx_t>(child_stats.cardinality, 1), DistinctCountSource::EXACT);
	default:
		return {};
	}
}

optional<RelationStats> RelationStatisticsHelper::ExtractDistinctStats(LogicalDistinct &distinct,
                                                                       const RelationStats &child_stats) {
	auto result = ProjectOutputStats(child_stats, distinct);
	if (!result) {
		return {};
	}
	vector<DistinctCount> distinct_counts;
	if (distinct.distinct_targets.empty()) {
		for (auto &column : child_stats.columns) {
			distinct_counts.push_back(column.distinct_count);
		}
	} else {
		for (auto &target : distinct.distinct_targets) {
			auto count = GetDistinctTargetCount(*target, child_stats);
			if (!count) {
				return result;
			}
			distinct_counts.push_back(*count);
		}
	}
	result->cardinality = EstimateDistinctCardinality(distinct_counts, child_stats.cardinality);
	for (auto &column : result->columns) {
		column.distinct_count.distinct_count = MinValue(column.distinct_count.distinct_count, result->cardinality);
	}
	return result;
}

RelationStats RelationStatisticsHelper::ExtractEmptyResultStats(LogicalEmptyResult &empty) {
	RelationStats result;
	result.cardinality = 0;
	result.table_name = Identifier(empty.GetName());
	result.stats_initialized = true;
	for (auto &binding : empty.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(0, DistinctCountSource::CARDINALITY),
		                            Identifier("empty_result_column"));
	}
	result.Verify(empty.GetColumnBindings());
	return result;
}

optional<RelationStats> RelationStatisticsHelper::ProjectOutputStats(const RelationStats &stats, LogicalOperator &op) {
	if (!stats.stats_initialized) {
		return {};
	}
	RelationStats result;
	result.cardinality = stats.cardinality;
	result.filter_strength = stats.filter_strength;
	result.stats_initialized = true;
	result.table_name = stats.table_name;
	for (auto &binding : op.GetColumnBindings()) {
		auto column = stats.GetColumnStats(binding);
		if (!column) {
			return {};
		}
		result.columns.push_back(*column);
	}
	result.Verify(op.GetColumnBindings());
	return result;
}

optional<RelationStats> RelationStatisticsHelper::RebindOutputStats(const RelationStats &stats, LogicalOperator &op) {
	auto bindings = op.GetColumnBindings();
	if (!stats.stats_initialized || bindings.size() != stats.columns.size()) {
		return {};
	}
	auto result = stats;
	for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
		result.columns[column_idx].binding = bindings[column_idx];
	}
	result.Verify(bindings);
	return result;
}

} // namespace duckdb
