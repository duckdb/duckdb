#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

ExpressionFilter::ExpressionFilter(unique_ptr<Expression> expr_p)
    : TableFilter(TableFilterType::EXPRESSION_FILTER), expr(std::move(expr_p)) {
}

const ExpressionFilter &ExpressionFilter::GetExpressionFilter(const TableFilter &filter, const char *context) {
	D_ASSERT(filter.filter_type == TableFilterType::EXPRESSION_FILTER);
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw InternalException("%s expected ExpressionFilter", context);
	}
	return filter.Cast<ExpressionFilter>();
}

ExpressionFilter &ExpressionFilter::GetExpressionFilter(TableFilter &filter, const char *context) {
	D_ASSERT(filter.filter_type == TableFilterType::EXPRESSION_FILTER);
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw InternalException("%s expected ExpressionFilter", context);
	}
	return filter.Cast<ExpressionFilter>();
}

unique_ptr<Expression> ExpressionFilter::CreateInExpression(unique_ptr<Expression> column, vector<Value> values) {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	result->children.push_back(std::move(column));
	for (auto &value : values) {
		result->children.push_back(make_uniq<BoundConstantExpression>(std::move(value)));
	}
	return std::move(result);
}

unique_ptr<Expression> ExpressionFilter::CreateNullCheckExpression(unique_ptr<Expression> column,
                                                                   ExpressionType expression_type) {
	D_ASSERT(expression_type == ExpressionType::OPERATOR_IS_NULL ||
	         expression_type == ExpressionType::OPERATOR_IS_NOT_NULL);
	auto result = make_uniq<BoundOperatorExpression>(expression_type, LogicalType::BOOLEAN);
	result->children.push_back(std::move(column));
	return result;
}

bool ExpressionFilter::EvaluateWithConstant(ClientContext &context, const Value &val) const {
	ExpressionExecutor executor(context, *expr);
	return EvaluateWithConstant(executor, val);
}

bool ExpressionFilter::EvaluateWithConstant(ExpressionExecutor &executor, const Value &val) const {
	DataChunk input;
	input.data.emplace_back(val);
	input.SetCardinality(1);

	SelectionVector sel(1);

	idx_t count = executor.SelectExpression(input, sel);
	return count > 0;
}

FilterPropagateResult ExpressionFilter::CheckStatistics(BaseStatistics &stats) const {
	if (stats.GetStatsType() == StatisticsType::GEOMETRY_STATS) {
		// Delegate to GeometryStats for geometry types
		return GeometryStats::CheckZonemap(stats, expr);
	}
	return CheckExpressionStatistics(*expr, stats);
}

static bool IsDirectColumnRef(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_REF;
}

static FilterPropagateResult CheckZonemapAgainstConstants(BaseStatistics &stats, ExpressionType comparison_type,
                                                          array_ptr<const Value> values) {
	D_ASSERT(values.size() > 0);
	switch (values[0].type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return NumericStats::CheckZonemap(stats, comparison_type, values);
	case PhysicalType::VARCHAR:
		if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
			return StringStats::CheckZonemap(stats, comparison_type, values);
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

static FilterPropagateResult CheckFunctionStatistics(const BoundFunctionExpression &func_expr, BaseStatistics &stats) {
	if (!func_expr.function.HasFilterPruneCallback()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	FunctionStatisticsPruneInput input(func_expr.bind_info.get(), stats);
	return func_expr.function.GetFilterPruneCallback()(input);
}

static FilterPropagateResult CheckComparisonStatistics(const BoundComparisonExpression &comp_expr,
                                                       BaseStatistics &stats) {
	if (!IsDirectColumnRef(*comp_expr.left) || comp_expr.right->type != ExpressionType::VALUE_CONSTANT) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &constant = comp_expr.right->Cast<BoundConstantExpression>().value;
	if (constant.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!stats.CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result = CheckZonemapAgainstConstants(stats, comp_expr.type, array_ptr<const Value>(&constant, 1));
	if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && stats.CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckNullOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats,
                                                         ExpressionType operator_type) {
	if (op_expr.children.empty() || !IsDirectColumnRef(*op_expr.children[0])) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (operator_type == ExpressionType::OPERATOR_IS_NULL) {
		if (!stats.CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!stats.CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	} else {
		if (!stats.CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!stats.CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

static FilterPropagateResult CheckInOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats) {
	if (op_expr.children.size() <= 1 || !IsDirectColumnRef(*op_expr.children[0])) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<Value> values;
	values.reserve(op_expr.children.size() - 1);
	for (idx_t i = 1; i < op_expr.children.size(); i++) {
		if (op_expr.children[i]->type != ExpressionType::VALUE_CONSTANT) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		auto &value = op_expr.children[i]->Cast<BoundConstantExpression>().value;
		if (!value.IsNull()) {
			values.push_back(value);
		}
	}
	if (values.empty()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!stats.CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result = CheckZonemapAgainstConstants(stats, ExpressionType::COMPARE_EQUAL,
	                                           array_ptr<const Value>(values.data(), values.size()));
	if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && stats.CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats) {
	switch (op_expr.type) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return CheckNullOperatorStatistics(op_expr, stats, op_expr.type);
	case ExpressionType::COMPARE_IN:
		return CheckInOperatorStatistics(op_expr, stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

static FilterPropagateResult CheckConjunctionStatistics(const BoundConjunctionExpression &conj, BaseStatistics &stats) {
	switch (conj.type) {
	case ExpressionType::CONJUNCTION_AND: {
		auto result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
		for (auto &child : conj.children) {
			auto prune_result = ExpressionFilter::CheckExpressionStatistics(*child, stats);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			if (prune_result != result) {
				result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}
		return result;
	}
	case ExpressionType::CONJUNCTION_OR:
		D_ASSERT(!conj.children.empty());
		for (auto &child : conj.children) {
			auto prune_result = ExpressionFilter::CheckExpressionStatistics(*child, stats);
			if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
				return FilterPropagateResult::FILTER_ALWAYS_TRUE;
			}
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

FilterPropagateResult ExpressionFilter::CheckExpressionStatistics(const Expression &expr, BaseStatistics &stats) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return CheckFunctionStatistics(expr.Cast<BoundFunctionExpression>(), stats);
	case ExpressionClass::BOUND_COMPARISON:
		return CheckComparisonStatistics(expr.Cast<BoundComparisonExpression>(), stats);
	case ExpressionClass::BOUND_OPERATOR:
		return CheckOperatorStatistics(expr.Cast<BoundOperatorExpression>(), stats);
	case ExpressionClass::BOUND_CONJUNCTION:
		return CheckConjunctionStatistics(expr.Cast<BoundConjunctionExpression>(), stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

unique_ptr<ExpressionFilter> ExpressionFilter::FromTableFilter(const TableFilter &filter, const LogicalType &col_type) {
	if (filter.filter_type == TableFilterType::EXPRESSION_FILTER) {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return make_uniq<ExpressionFilter>(expr_filter.expr->Copy());
	}
	if (col_type == LogicalType::ANY) {
		throw InternalException("ExpressionFilter::FromTableFilter requires the actual column type");
	}
	storage_t col_idx = 0;
	auto col_ref = make_uniq<BoundReferenceExpression>(col_type, col_idx);
	auto expr = filter.ToExpression(*col_ref);
	return make_uniq<ExpressionFilter>(std::move(expr));
}

string ExpressionFilter::ExpressionToFriendlyString(const Expression &expression, const string &column_name) {
	// Default: use standard expression ToString with column name substitution
	auto expr_copy = expression.Copy();
	auto name_expr = make_uniq<BoundReferenceExpression>(column_name, LogicalType::INVALID, 0ULL);
	ReplaceExpressionRecursive(expr_copy, *name_expr, ExpressionType::BOUND_REF);
	return expr_copy->ToString();
}

string ExpressionFilter::ToString(const string &column_name) const {
	return ExpressionToFriendlyString(*expr, column_name);
}

void ExpressionFilter::ReplaceExpressionRecursive(unique_ptr<Expression> &expr, const Expression &column,
                                                  ExpressionType replace_type) {
	if (expr->type == replace_type) {
		expr = column.Copy();
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceExpressionRecursive(child, column, replace_type); });
}

unique_ptr<Expression> ExpressionFilter::ToExpression(const Expression &column) const {
	auto expr_copy = expr->Copy();
	ReplaceExpressionRecursive(expr_copy, column);
	return expr_copy;
}

bool ExpressionFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ExpressionFilter>();
	return other.expr->Equals(*expr);
}

unique_ptr<TableFilter> ExpressionFilter::Copy() const {
	return make_uniq<ExpressionFilter>(expr->Copy());
}

} // namespace duckdb
