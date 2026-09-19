#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

static idx_t MultiplyCardinalities(idx_t left, idx_t right) {
	idx_t result;
	if (!TryMultiplyOperator::Operation(left, right, result)) {
		return NumericLimits<idx_t>::Maximum();
	}
	return result;
}

static optional<idx_t> GetCollectionValueCardinality(const Value &value) {
	if (value.IsNull()) {
		return 0;
	}
	switch (value.type().id()) {
	case LogicalTypeId::LIST:
		return ListValue::GetChildren(value).size();
	case LogicalTypeId::ARRAY:
		return ArrayValue::GetChildren(value).size();
	default:
		return {};
	}
}

static optional<idx_t> GetUnnestExpressionCardinality(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return GetCollectionValueCardinality(expr.Cast<BoundConstantExpression>().GetValue());
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return {};
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	if (function.Function().GetName() == "list_value") {
		return function.GetChildren().size();
	}
	if (BoundCastExpression::IsCast(function)) {
		auto &child = BoundCastExpression::Child(function);
		if (child.GetReturnType().id() == LogicalTypeId::ARRAY) {
			return ArrayType::GetSize(child.GetReturnType());
		}
		return GetUnnestExpressionCardinality(child);
	}
	return {};
}

static idx_t GetUnnestCardinality(const LogicalUnnest &unnest) {
	idx_t cardinality = 0;
	for (auto &expression : unnest.expressions) {
		auto &child = expression->Cast<BoundUnnestExpression>().Child();
		// Multiple UNNEST expressions are emitted side-by-side and padded to the longest collection.
		cardinality = MaxValue(cardinality, GetUnnestExpressionCardinality(*child).value_or(
		                                        RelationStatisticsHelper::DEFAULT_UNNEST_CARDINALITY));
	}
	return cardinality;
}

idx_t RelationStatisticsHelper::EstimateUnnestCardinality(const LogicalGet &get) {
	if (get.parameters.size() == 1) {
		auto cardinality = GetCollectionValueCardinality(get.parameters[0]);
		if (cardinality) {
			return *cardinality;
		}
	}
	if (get.input_table_types.size() == 1 && get.input_table_types[0].id() == LogicalTypeId::ARRAY) {
		return ArrayType::GetSize(get.input_table_types[0]);
	}
	return DEFAULT_UNNEST_CARDINALITY;
}

static void SetUnnestGetCardinality(LogicalGet &get, RelationStats &stats, idx_t input_cardinality) {
	stats.cardinality =
	    MultiplyCardinalities(input_cardinality, RelationStatisticsHelper::EstimateUnnestCardinality(get));
	for (auto &column : stats.columns) {
		if (column.binding.table_index == get.table_index &&
		    column.distinct_count.source == DistinctCountSource::CARDINALITY) {
			column.distinct_count = DistinctCount(stats.cardinality, DistinctCountSource::CARDINALITY);
		}
	}
}

static optional<RelationStats>
ProjectChildStats(LogicalOperator &op, const vector<reference<const RelationStats>> &children, idx_t cardinality) {
	RelationStats result;
	result.cardinality = cardinality;
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	for (auto &binding : op.GetColumnBindings()) {
		optional_ptr<const RelationColumnStats> source;
		for (auto &child : children) {
			source = child.get().GetColumnStats(binding);
			if (source) {
				break;
			}
		}
		if (!source) {
			return {};
		}
		result.columns.emplace_back(binding, source->distinct_count, source->name);
	}
	result.Verify(op.GetColumnBindings());
	return result;
}

static idx_t JoinCardinality(LogicalComparisonJoin &join, const RelationStats &left, const RelationStats &right) {
	switch (join.join_type) {
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		return right.cardinality;
	case JoinType::ANTI:
	case JoinType::SEMI:
	case JoinType::SINGLE:
	case JoinType::MARK:
		return left.cardinality;
	default:
		return MaxValue(left.cardinality, right.cardinality);
	}
}

static optional<RelationStats> ExtractGetWithChildStats(LogicalGet &get, ClientContext &context,
                                                        const RelationStats &child_stats) {
	auto result = RelationStatisticsHelper::ExtractGetStats(get, context);
	if (ExpressionBinder::IsUnnestFunction(get.function.name)) {
		SetUnnestGetCardinality(get, result, child_stats.cardinality);
	} else {
		result.cardinality = child_stats.cardinality;
	}
	for (auto &binding : get.GetColumnBindings()) {
		if (binding.table_index == get.table_index) {
			continue;
		}
		auto child_column = child_stats.GetColumnStats(binding);
		if (!child_column) {
			return {};
		}
		result.columns.emplace_back(binding, child_column->distinct_count, child_column->name);
	}
	result.Verify(get.GetColumnBindings());
	return result;
}

static optional<RelationStats> ExtractUnnestStats(LogicalOperator &op, const RelationStats &child_stats) {
	auto &unnest = op.Cast<LogicalUnnest>();
	RelationStats result;
	result.cardinality = MultiplyCardinalities(child_stats.cardinality, GetUnnestCardinality(unnest));
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	for (auto &binding : op.GetColumnBindings()) {
		auto child_column = child_stats.GetColumnStats(binding);
		if (child_column) {
			result.columns.emplace_back(binding, child_column->distinct_count, child_column->name);
		} else if (binding.table_index == unnest.unnest_index) {
			result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
			                            Identifier("unnest"));
		} else {
			return {};
		}
	}
	result.Verify(op.GetColumnBindings());
	return result;
}

static optional<RelationStats> ExtractComparisonJoinStats(LogicalComparisonJoin &join,
                                                          const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != 2) {
		return {};
	}
	auto cardinality = JoinCardinality(join, child_stats[0], child_stats[1]);
	auto result = ProjectChildStats(join, child_stats, cardinality);
	if (result || join.join_type != JoinType::MARK) {
		return result;
	}

	RelationStats mark_result;
	mark_result.cardinality = cardinality;
	mark_result.stats_initialized = true;
	mark_result.table_name = Identifier(join.GetName());
	for (auto &binding : join.GetColumnBindings()) {
		auto column = child_stats[0].get().GetColumnStats(binding);
		if (column) {
			mark_result.columns.emplace_back(binding, column->distinct_count, column->name);
		} else if (binding.table_index == join.mark_index) {
			mark_result.columns.emplace_back(
			    binding, DistinctCount(MinValue<idx_t>(cardinality, 3), DistinctCountSource::CARDINALITY),
			    Identifier("mark"));
		} else {
			return {};
		}
	}
	mark_result.Verify(join.GetColumnBindings());
	return mark_result;
}

static optional<RelationStats> ExtractCrossProductStats(LogicalOperator &op,
                                                        const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != 2) {
		return {};
	}
	auto cardinality = MultiplyCardinalities(child_stats[0].get().cardinality, child_stats[1].get().cardinality);
	return ProjectChildStats(op, child_stats, cardinality);
}

RelationStats RelationStatisticsHelper::ExtractExplainStats(LogicalOperator &op) {
	RelationStats result;
	result.cardinality = 3;
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	for (auto &binding : op.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier("explain"));
	}
	result.Verify(op.GetColumnBindings());
	return result;
}

optional<RelationStats>
RelationStatisticsHelper::ExtractOperatorStats(LogicalOperator &op, ClientContext &context,
                                               const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != op.children.size()) {
		return {};
	}
	for (idx_t child_idx = 0; child_idx < child_stats.size(); child_idx++) {
		auto &stats = child_stats[child_idx].get();
		if (!stats.stats_initialized || !stats.MatchesBindings(op.children[child_idx]->GetColumnBindings())) {
			return {};
		}
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		if (child_stats.empty()) {
			return ExtractGetStats(op.Cast<LogicalGet>(), context);
		}
		return child_stats.size() == 1 ? ExtractGetWithChildStats(op.Cast<LogicalGet>(), context, child_stats[0].get())
		                               : optional<RelationStats>();
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return ExtractDelimGetStats(op.Cast<LogicalDelimGet>(), context);
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return ExtractDummyScanStats(op.Cast<LogicalDummyScan>(), context);
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return ExtractExpressionGetStats(op.Cast<LogicalExpressionGet>(), context);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return ExtractColumnDataGetStats(op.Cast<LogicalColumnDataGet>(), context);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return ExtractProjectionStats(op.Cast<LogicalProjection>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return ExtractAggregationStats(op.Cast<LogicalAggregate>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_WINDOW:
		return ExtractWindowStats(op.Cast<LogicalWindow>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return ExtractDistinctStats(op.Cast<LogicalDistinct>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (child_stats.size() != 1) {
			return {};
		}
		auto cardinality = child_stats[0].get().cardinality;
		if (cardinality > 0) {
			cardinality = MaxValue<idx_t>(LossyNumericCast<idx_t>(double(cardinality) * DEFAULT_SELECTIVITY), 1);
		}
		return ProjectChildStats(op, child_stats, cardinality);
	}
	case LogicalOperatorType::LOGICAL_UNNEST:
		return ExtractUnnestStats(op, child_stats[0].get());
	case LogicalOperatorType::LOGICAL_LIMIT: {
		if (child_stats.size() != 1) {
			return {};
		}
		auto cardinality = child_stats[0].get().cardinality;
		auto &limit = op.Cast<LogicalLimit>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			cardinality = MinValue(cardinality, limit.limit_val.GetConstantValue());
		}
		return ProjectChildStats(op, child_stats, cardinality);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return ExtractComparisonJoinStats(op.Cast<LogicalComparisonJoin>(), child_stats);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return ExtractCrossProductStats(op, child_stats);
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return ExtractEmptyResultStats(op.Cast<LogicalEmptyResult>());
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		return ExtractExplainStats(op);
	default:
		return {};
	}
}

} // namespace duckdb
