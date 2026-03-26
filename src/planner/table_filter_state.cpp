#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

static bool IsSimpleFilterColumnRef(const Expression &expression) {
	return expression.type == ExpressionType::BOUND_REF ||
	       expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
}

static bool HasSupportedFastPathComparisonType(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool HasSupportedFastPathPhysicalType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::VARCHAR:
	case PhysicalType::BOOL:
		return true;
	default:
		return false;
	}
}

static bool CanUseConstantComparisonFastPath(const Expression &column, ExpressionType comparison_type,
                                             const Value &constant) {
	if (!HasSupportedFastPathComparisonType(comparison_type)) {
		return false;
	}
	if (constant.IsNull()) {
		return true;
	}
	return HasSupportedFastPathPhysicalType(column.return_type);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	auto initialize_executor = [&]() {
		executor = make_uniq<ExpressionExecutor>(context);
		executor->AddExpression(expression);
	};
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (conjunction.type == ExpressionType::CONJUNCTION_AND && !conjunction.children.empty()) {
			child_states.reserve(conjunction.children.size());
			for (auto &child : conjunction.children) {
				child_states.push_back(make_uniq<ExpressionFilterState>(context, *child));
			}
			if (conjunction.children.size() > 1) {
				adaptive_filter = make_uniq<AdaptiveFilter>(expression);
			}
			return;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = expression.Cast<BoundComparisonExpression>();
		if (IsSimpleFilterColumnRef(*comparison.left) && comparison.right->type == ExpressionType::VALUE_CONSTANT &&
		    CanUseConstantComparisonFastPath(*comparison.left, comparison.GetExpressionType(),
		                                     comparison.right->Cast<BoundConstantExpression>().value)) {
			fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
			comparison_type = comparison.GetExpressionType();
			constant = comparison.right->Cast<BoundConstantExpression>().value;
			initialize_executor();
			return;
		}
		if (IsSimpleFilterColumnRef(*comparison.right) && comparison.left->type == ExpressionType::VALUE_CONSTANT &&
		    CanUseConstantComparisonFastPath(*comparison.right,
		                                     FlipComparisonExpression(comparison.GetExpressionType()),
		                                     comparison.left->Cast<BoundConstantExpression>().value)) {
			fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
			comparison_type = FlipComparisonExpression(comparison.GetExpressionType());
			constant = comparison.left->Cast<BoundConstantExpression>().value;
			initialize_executor();
			return;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op = expression.Cast<BoundOperatorExpression>();
		if (op.children.size() == 1 && IsSimpleFilterColumnRef(*op.children[0])) {
			if (expression.type == ExpressionType::OPERATOR_IS_NULL) {
				fast_path = ExpressionFilterFastPath::IS_NULL;
				initialize_executor();
				return;
			}
			if (expression.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
				fast_path = ExpressionFilterFastPath::IS_NOT_NULL;
				initialize_executor();
				return;
			}
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (function.function.name == BloomFilterScalarFun::NAME && function.bind_info) {
			auto &bind_data = function.bind_info->Cast<BloomFilterFunctionData>();
			fast_path = ExpressionFilterFastPath::BLOOM_FILTER;
			bloom_filter = bind_data.filter;
			bloom_filters_null_values = bind_data.filters_null_values;
			if (bind_data.filter && bind_data.n_vectors_to_check != 0) {
				EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
			}
			initialize_executor();
			return;
		}
		if (function.function.name == SelectivityOptionalFilterScalarFun::NAME && function.bind_info) {
			auto &bind_data = function.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			if (bind_data.child_filter_expr) {
				fast_path = ExpressionFilterFastPath::SELECTIVITY_OPTIONAL;
				EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
				selectivity_child_state = make_uniq<ExpressionFilterState>(context, *bind_data.child_filter_expr);
				initialize_executor();
				return;
			}
		}
		if (function.function.name == OptionalFilterScalarFun::NAME && function.bind_info) {
			fast_path = ExpressionFilterFastPath::OPTIONAL;
			initialize_executor();
			return;
		}
		if (function.function.name == PerfectHashJoinScalarFun::NAME && function.bind_info) {
			auto &bind_data = function.bind_info->Cast<PerfectHashJoinFunctionData>();
			if (!bind_data.executor) {
				initialize_executor();
				return;
			}
			fast_path = ExpressionFilterFastPath::PERFECT_HASH_JOIN;
			if (bind_data.n_vectors_to_check != 0) {
				EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
			}
			initialize_executor();
			return;
		}
		if (function.function.name == PrefixRangeScalarFun::NAME && function.bind_info) {
			auto &bind_data = function.bind_info->Cast<PrefixRangeFunctionData>();
			if (!bind_data.filter) {
				initialize_executor();
				return;
			}
			fast_path = ExpressionFilterFastPath::PREFIX_RANGE;
			if (bind_data.n_vectors_to_check != 0) {
				EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
			}
			initialize_executor();
			return;
		}
		if (function.function.name == DynamicFilterScalarFun::NAME && function.bind_info && function.children.size() == 1) {
			auto &bind_data = function.bind_info->Cast<DynamicFilterFunctionData>();
			if (!bind_data.filter_data || !HasSupportedFastPathComparisonType(bind_data.filter_data->comparison_type) ||
			    !HasSupportedFastPathPhysicalType(function.children[0]->return_type)) {
				initialize_executor();
				return;
			}
			fast_path = ExpressionFilterFastPath::DYNAMIC_FILTER;
			dynamic_filter_data = bind_data.filter_data;
			initialize_executor();
			return;
		}
	}
	initialize_executor();
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
