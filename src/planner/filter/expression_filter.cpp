#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

ExpressionFilter::ExpressionFilter(unique_ptr<Expression> expr_p)
    : TableFilter(TableFilterType::EXPRESSION_FILTER), expr(std::move(expr_p)) {
}

bool ExpressionFilter::EvaluateWithConstant(ClientContext &context, const Value &val) {
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
	// Geometry stats need the unique_ptr
	if (stats.GetStatsType() == StatisticsType::GEOMETRY_STATS) {
		return GeometryStats::CheckZonemap(stats, expr);
	}
	return CheckExpressionStatistics(*expr, stats);
}

static bool IsDirectColumnRef(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_REF;
}

FilterPropagateResult ExpressionFilter::CheckExpressionStatistics(const Expression &expr, BaseStatistics &stats) {
	// Check bound function expressions (internal functions with FilterPruneCallback)
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		if (func_expr.function.HasFilterPruneCallback()) {
			FunctionStatisticsPruneInput input(func_expr.bind_info.get(), stats);
			return func_expr.function.GetFilterPruneCallback()(input);
		}
	}

	// Recognize comparison expressions (from ConstantFilter conversion)
	// Only apply when left side is a direct column reference — stats are column-level
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		if (IsDirectColumnRef(*comp_expr.left) && comp_expr.right->type == ExpressionType::VALUE_CONSTANT) {
			auto &constant_expr = comp_expr.right->Cast<BoundConstantExpression>();
			auto &constant = constant_expr.value;
			if (constant.IsNull()) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			if (!stats.CanHaveNoNull()) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			FilterPropagateResult result;
			switch (constant.type().InternalType()) {
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
				result = NumericStats::CheckZonemap(stats, expr.type, array_ptr<const Value>(&constant, 1));
				break;
			case PhysicalType::VARCHAR:
				if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
					result = StringStats::CheckZonemap(stats, expr.type, array_ptr<const Value>(&constant, 1));
				} else {
					return FilterPropagateResult::NO_PRUNING_POSSIBLE;
				}
				break;
			default:
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && stats.CanHaveNull()) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			return result;
		}
	}

	// Recognize IS NULL expressions — only when applied directly to the column
	if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (!op_expr.children.empty() && IsDirectColumnRef(*op_expr.children[0])) {
			if (!stats.CanHaveNull()) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			if (!stats.CanHaveNoNull()) {
				return FilterPropagateResult::FILTER_ALWAYS_TRUE;
			}
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// Recognize IS NOT NULL expressions — only when applied directly to the column
	if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (!op_expr.children.empty() && IsDirectColumnRef(*op_expr.children[0])) {
			if (!stats.CanHaveNoNull()) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			if (!stats.CanHaveNull()) {
				return FilterPropagateResult::FILTER_ALWAYS_TRUE;
			}
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// Recognize IN expressions — only when first child is a direct column reference
	if (expr.type == ExpressionType::COMPARE_IN) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (op_expr.children.size() > 1 && IsDirectColumnRef(*op_expr.children[0])) {
			// Collect the constant values
			vector<Value> values;
			bool all_constants = true;
			for (idx_t i = 1; i < op_expr.children.size(); i++) {
				if (op_expr.children[i]->type == ExpressionType::VALUE_CONSTANT) {
					auto &const_expr = op_expr.children[i]->Cast<BoundConstantExpression>();
					values.push_back(const_expr.value);
				} else {
					all_constants = false;
					break;
				}
			}
			if (all_constants && !values.empty()) {
				if (!stats.CanHaveNoNull()) {
					return FilterPropagateResult::FILTER_ALWAYS_FALSE;
				}
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
					return NumericStats::CheckZonemap(stats, ExpressionType::COMPARE_EQUAL,
					                                  array_ptr<const Value>(values.data(), values.size()));
				case PhysicalType::VARCHAR:
					if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
						return StringStats::CheckZonemap(stats, ExpressionType::COMPARE_EQUAL,
						                                 array_ptr<const Value>(values.data(), values.size()));
					}
					return FilterPropagateResult::NO_PRUNING_POSSIBLE;
				default:
					return FilterPropagateResult::NO_PRUNING_POSSIBLE;
				}
			}
		}
	}

	// Recognize AND conjunctions
	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		auto result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
		for (auto &child : conj.children) {
			auto prune_result = CheckExpressionStatistics(*child, stats);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			} else if (prune_result != result) {
				result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}
		return result;
	}

	// Recognize OR conjunctions
	if (expr.type == ExpressionType::CONJUNCTION_OR) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		D_ASSERT(!conj.children.empty());
		for (auto &child : conj.children) {
			auto prune_result = CheckExpressionStatistics(*child, stats);
			if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
				return FilterPropagateResult::FILTER_ALWAYS_TRUE;
			}
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

static LogicalType InferFilterColumnType(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return constant_filter.constant.type();
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		if (!in_filter.values.empty()) {
			return in_filter.values[0].type();
		}
		return LogicalType::ANY;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : conj.child_filters) {
			auto child_type = InferFilterColumnType(*child);
			if (child_type != LogicalType::ANY) {
				return child_type;
			}
		}
		return LogicalType::ANY;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : conj.child_filters) {
			auto child_type = InferFilterColumnType(*child);
			if (child_type != LogicalType::ANY) {
				return child_type;
			}
		}
		return LogicalType::ANY;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		if (opt.child_filter) {
			return InferFilterColumnType(*opt.child_filter);
		}
		return LogicalType::ANY;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		// StructFilter needs the root struct column type, which can't be inferred from the leaf filter
		// Callers must pass the column type explicitly
		return LogicalType::ANY;
	}
	default:
		return LogicalType::ANY;
	}
}

unique_ptr<ExpressionFilter> ExpressionFilter::FromTableFilter(const TableFilter &filter, const LogicalType &col_type) {
	if (filter.filter_type == TableFilterType::EXPRESSION_FILTER) {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return make_uniq<ExpressionFilter>(expr_filter.expr->Copy());
	}
	auto inferred_type = InferFilterColumnType(filter);
	auto &effective_type = (inferred_type != LogicalType::ANY) ? inferred_type : col_type;
	auto col_ref = make_uniq<BoundReferenceExpression>(effective_type, 0);
	auto expr = filter.ToExpression(*col_ref);
	return make_uniq<ExpressionFilter>(std::move(expr));
}

static void ReplaceStructExtractAt(unique_ptr<Expression> &expr, const string &column_name) {
	// Recursively process children first
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&column_name](unique_ptr<Expression> &child) { ReplaceStructExtractAt(child, column_name); });

	// Check if this is a struct_extract_at function call
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expr->Cast<BoundFunctionExpression>();
		if (func_expr.function.name == "struct_extract_at" && !func_expr.children.empty()) {
			auto &child_type = func_expr.children[0]->return_type;
			if (child_type.id() == LogicalTypeId::STRUCT) {
				auto &bind_data = func_expr.bind_info->Cast<StructExtractBindData>();
				auto &field_name = StructType::GetChildName(child_type, bind_data.index);
				// Build the dot-notation name from the child
				string base_name;
				if (func_expr.children[0]->type == ExpressionType::BOUND_REF) {
					base_name = column_name;
				} else {
					base_name = func_expr.children[0]->GetName();
				}
				auto alias = base_name + "." + field_name;
				auto replacement = make_uniq<BoundReferenceExpression>(alias, func_expr.return_type, 0ULL);
				expr = std::move(replacement);
			}
		}
	}
}

string ExpressionFilter::InternalFunctionToString(const BoundFunctionExpression &func_expr,
                                                  const string &column_name) {
	auto &func_name = func_expr.function.name;
	if (func_name == BloomFilterScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<BloomFilterFunctionData>();
		return column_name + " IN BF(" + data.key_column_name + ")";
	} else if (func_name == PerfectHashJoinScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<PerfectHashJoinFunctionData>();
		return column_name + " IN PHJ(" + data.key_column_name + ")";
	} else if (func_name == PrefixRangeScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<PrefixRangeFunctionData>();
		return column_name + " IN PRF(" + data.key_column_name + ")";
	} else if (func_name == DynamicFilterScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<DynamicFilterFunctionData>();
		if (data.filter_data) {
			return "Dynamic Filter (" + column_name + ")";
		} else {
			return "Dynamic Filter";
		}
	} else if (func_name == OptionalFilterScalarFun::NAME) {
		auto &data = func_expr.bind_info->Cast<OptionalFilterFunctionData>();
		return "optional: " + ExpressionToFriendlyString(*data.child_filter_expr, column_name);
	}
	return string();
}

string ExpressionFilter::ExpressionToFriendlyString(const Expression &expression, const string &column_name) {
	// Handle internal functions
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expression.Cast<BoundFunctionExpression>();
		auto result = InternalFunctionToString(func_expr, column_name);
		if (!result.empty()) {
			return result;
		}
	}
	// Handle conjunctions (AND/OR) that may contain internal functions
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expression.Cast<BoundConjunctionExpression>();
		bool has_internal = false;
		for (auto &child : conj.children) {
			if (child->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &func = child->Cast<BoundFunctionExpression>();
				if (StringUtil::StartsWith(func.function.name, "__internal_tablefilter_")) {
					has_internal = true;
					break;
				}
			}
		}
		if (has_internal) {
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
	// Convert struct_extract_at to dot notation BEFORE replacing BoundRefs (needs type info)
	ReplaceStructExtractAt(expr_copy, column_name);
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


bool ExpressionFilter::ContainsInternalFunction(const Expression &expr, const string &func_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == func_name) {
			return true;
		}
		// Check inside optional filter bind data (it wraps a child expression)
		if (func.function.name == OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalFunction(*data.child_filter_expr, func_name)) {
				return true;
			}
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(const_cast<Expression &>(expr), [&](unique_ptr<Expression> &child) {
		if (!found && child) {
			found = ContainsInternalFunction(*child, func_name);
		}
	});
	return found;
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
