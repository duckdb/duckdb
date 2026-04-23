#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"
#include "duckdb/planner/filter/prefix_range_filter.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
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
	return std::move(result);
}

static bool IsOptionalInternalFunction(const BoundFunctionExpression &func) {
	return func.function.name == OptionalFilterScalarFun::NAME ||
	       func.function.name == SelectivityOptionalFilterScalarFun::NAME;
}

static bool IsOptionalExpressionInternal(const Expression &expr, bool recurse_through_and) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return IsOptionalInternalFunction(expr.Cast<BoundFunctionExpression>());
	}
	if (!recurse_through_and || expr.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION ||
	    expr.type != ExpressionType::CONJUNCTION_AND) {
		return false;
	}
	auto &conj = expr.Cast<BoundConjunctionExpression>();
	if (conj.children.empty()) {
		return false;
	}
	for (auto &child : conj.children) {
		if (!IsOptionalExpressionInternal(*child, true)) {
			return false;
		}
	}
	return true;
}

static bool ContainsInternalTableFilterFunction(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (TableFilterFunctions::IsTableFilterFunction(func.function)) {
			return true;
		}
		if (func.function.name == OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalTableFilterFunction(*data.child_filter_expr)) {
				return true;
			}
		}
		if (func.function.name == SelectivityOptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalTableFilterFunction(*data.child_filter_expr)) {
				return true;
			}
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = ContainsInternalTableFilterFunction(child);
		}
	});
	return found;
}

bool ExpressionFilter::EvaluateWithConstant(ClientContext &context, const Value &val) const {
	ExpressionExecutor executor(context, *expr);
	return EvaluateWithConstant(executor, val);
}

bool ExpressionFilter::EvaluateWithConstant(ExpressionExecutor &executor, const Value &val) const {
	DataChunk input;
	input.data.emplace_back(val, count_t(1));
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

static FilterPropagateResult CheckZonemapAgainstConstants(const BaseStatistics &stats, ExpressionType comparison_type,
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

static optional_ptr<const BaseStatistics> TryGetFilterStats(const Expression &expr, const BaseStatistics &stats,
                                                            vector<unique_ptr<BaseStatistics>> &owned_stats) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return &stats;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.children.empty()) {
			return nullptr;
		}
		auto child_stats = TryGetFilterStats(*func.children[0], stats, owned_stats);
		if (!child_stats || child_stats->GetType().id() != LogicalTypeId::STRUCT) {
			return nullptr;
		}
		owned_stats.push_back(StructStats::GetChildStats(*child_stats, child_idx).ToUnique());
		return owned_stats.back().get();
	}
	default:
		return nullptr;
	}
}

static FilterPropagateResult CheckComparisonStatistics(const BoundFunctionExpression &comp_expr,
                                                       BaseStatistics &stats) {
	vector<unique_ptr<BaseStatistics>> owned_stats;
	optional_ptr<const BaseStatistics> filter_stats;
	optional_ptr<const BoundConstantExpression> constant_expr;
	auto comparison_type = comp_expr.GetExpressionType();
	auto &left = BoundComparisonExpression::Left(comp_expr);
	auto &right = BoundComparisonExpression::Right(comp_expr);
	if (right.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		filter_stats = TryGetFilterStats(left, stats, owned_stats);
		constant_expr = &right.Cast<BoundConstantExpression>();
	} else if (left.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		filter_stats = TryGetFilterStats(right, stats, owned_stats);
		constant_expr = &left.Cast<BoundConstantExpression>();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!filter_stats || !constant_expr) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &constant = constant_expr->value;
	if (constant.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!filter_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result = CheckZonemapAgainstConstants(*filter_stats, comparison_type, array_ptr<const Value>(&constant, 1));
	if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && filter_stats->CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckFunctionStatistics(const BoundFunctionExpression &func_expr, BaseStatistics &stats) {
	if (BoundComparisonExpression::IsComparison(func_expr.GetExpressionType())) {
		return CheckComparisonStatistics(func_expr, stats);
	}
	if (!func_expr.function.HasFilterPruneCallback()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	FunctionStatisticsPruneInput input(func_expr.bind_info.get(), stats);
	return func_expr.function.GetFilterPruneCallback()(input);
}

static FilterPropagateResult CheckNullOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats,
                                                         ExpressionType operator_type) {
	if (op_expr.children.empty()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<unique_ptr<BaseStatistics>> owned_stats;
	auto filter_stats = TryGetFilterStats(*op_expr.children[0], stats, owned_stats);
	if (!filter_stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (operator_type == ExpressionType::OPERATOR_IS_NULL) {
		if (!filter_stats->CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!filter_stats->CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	} else {
		if (!filter_stats->CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!filter_stats->CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

static FilterPropagateResult CheckInOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats) {
	if (op_expr.children.size() <= 1) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<unique_ptr<BaseStatistics>> owned_stats;
	auto filter_stats = TryGetFilterStats(*op_expr.children[0], stats, owned_stats);
	if (!filter_stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<Value> values;
	values.reserve(op_expr.children.size() - 1);
	for (idx_t i = 1; i < op_expr.children.size(); i++) {
		if (op_expr.children[i]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
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
	if (!filter_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result = CheckZonemapAgainstConstants(*filter_stats, ExpressionType::COMPARE_EQUAL,
	                                           array_ptr<const Value>(values.data(), values.size()));
	if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && filter_stats->CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckOperatorStatistics(const BoundOperatorExpression &op_expr, BaseStatistics &stats) {
	switch (op_expr.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return CheckNullOperatorStatistics(op_expr, stats, op_expr.GetExpressionType());
	case ExpressionType::COMPARE_IN:
		return CheckInOperatorStatistics(op_expr, stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

static FilterPropagateResult CheckConjunctionStatistics(const BoundConjunctionExpression &conj, BaseStatistics &stats) {
	switch (conj.GetExpressionType()) {
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
	case ExpressionClass::BOUND_OPERATOR:
		return CheckOperatorStatistics(expr.Cast<BoundOperatorExpression>(), stats);
	case ExpressionClass::BOUND_CONJUNCTION:
		return CheckConjunctionStatistics(expr.Cast<BoundConjunctionExpression>(), stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

bool ExpressionFilter::ContainsInternalFunction(const Expression &expr, const string &func_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == func_name) {
			return true;
		}
		if (func.function.name == OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalFunction(*data.child_filter_expr, func_name)) {
				return true;
			}
		}
		if (func.function.name == SelectivityOptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalFunction(*data.child_filter_expr, func_name)) {
				return true;
			}
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = ContainsInternalFunction(child, func_name);
		}
	});
	return found;
}

bool ExpressionFilter::IsOptionalExpression(const Expression &expr) {
	return IsOptionalExpressionInternal(expr, true);
}

bool ExpressionFilter::IsRootOptionalExpression(const Expression &expr) {
	return IsOptionalExpressionInternal(expr, false);
}

bool ExpressionFilter::IsOptionalFilter(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::IsOptionalFilter");
	return IsOptionalExpression(*expr_filter.expr);
}

bool ExpressionFilter::IsRootOptionalFilter(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::IsRootOptionalFilter");
	return IsRootOptionalExpression(*expr_filter.expr);
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

string ExpressionFilter::InternalFunctionToString(const BoundFunctionExpression &func_expr, const string &column_name) {
	auto &func_name = func_expr.function.name;
	if (func_name == BloomFilterScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<BloomFilterFunctionData>();
		return BloomFilterScalarFun::ToString(column_name, data.key_column_name);
	} else if (func_name == PerfectHashJoinScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<PerfectHashJoinFunctionData>();
		return PerfectHashJoinScalarFun::ToString(column_name, data.key_column_name);
	} else if (func_name == PrefixRangeScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<PrefixRangeFunctionData>();
		return PrefixRangeScalarFun::ToString(column_name, data.key_column_name);
	} else if (func_name == DynamicFilterScalarFun::NAME) {
		const auto has_filter_data =
		    func_expr.bind_info && func_expr.bind_info->Cast<DynamicFilterFunctionData>().filter_data;
		return DynamicFilterScalarFun::ToString(column_name, has_filter_data);
	} else if (func_name == OptionalFilterScalarFun::NAME) {
		string child_filter_string;
		if (func_expr.bind_info) {
			auto &data = func_expr.bind_info->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				child_filter_string = ExpressionToFriendlyString(*data.child_filter_expr, column_name);
			}
		}
		return OptionalFilterScalarFun::ToString(child_filter_string);
	} else if (func_name == SelectivityOptionalFilterScalarFun::NAME) {
		string child_filter_string;
		if (func_expr.bind_info) {
			auto &data = func_expr.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				child_filter_string = ExpressionToFriendlyString(*data.child_filter_expr, column_name);
			}
		}
		return SelectivityOptionalFilterScalarFun::ToString(child_filter_string);
	}
	return string();
}

string ExpressionFilter::ExpressionToFriendlyString(const Expression &expression, const string &column_name) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expression.Cast<BoundFunctionExpression>();
		auto result = InternalFunctionToString(func_expr, column_name);
		if (!result.empty()) {
			return result;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expression.Cast<BoundConjunctionExpression>();
		if (ContainsInternalTableFilterFunction(expression)) {
			string result = "(";
			for (idx_t i = 0; i < conj.children.size(); i++) {
				if (i > 0) {
					result += conj.type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
				}
				result += ExpressionToFriendlyString(*conj.children[i], column_name);
			}
			result += ")";
			return result;
		}
	}
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
	if (expr->GetExpressionType() == replace_type) {
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

bool ExpressionFilter::Equals(const ExpressionFilter &other_p) const {
	auto &other = other_p.Cast<ExpressionFilter>();
	return other.expr->Equals(*expr);
}

unique_ptr<ExpressionFilter> ExpressionFilter::Copy() const {
	return make_uniq<ExpressionFilter>(expr->Copy());
}

} // namespace duckdb
