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

static void InitializeExecutor(ClientContext &context, const Expression &expression, ExpressionFilterState &state) {
	state.executor = make_uniq<ExpressionExecutor>(context);
	state.executor->AddExpression(expression);
}

static bool TryInitializeConjunctionState(ClientContext &context, const Expression &expression,
                                          ExpressionFilterState &state) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION) {
		return false;
	}
	auto &conjunction = expression.Cast<BoundConjunctionExpression>();
	if (conjunction.type != ExpressionType::CONJUNCTION_AND || conjunction.children.empty()) {
		return false;
	}
	state.child_states.reserve(conjunction.children.size());
	for (auto &child : conjunction.children) {
		state.child_states.push_back(make_uniq<ExpressionFilterState>(context, *child));
	}
	if (conjunction.children.size() > 1) {
		state.adaptive_filter = make_uniq<AdaptiveFilter>(expression);
	}
	return true;
}

static bool TryInitializeComparisonState(ClientContext &context, const Expression &expression,
                                         ExpressionFilterState &state) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &comparison = expression.Cast<BoundComparisonExpression>();
	if (IsSimpleFilterColumnRef(*comparison.left) && comparison.right->type == ExpressionType::VALUE_CONSTANT &&
	    CanUseConstantComparisonFastPath(*comparison.left, comparison.GetExpressionType(),
	                                     comparison.right->Cast<BoundConstantExpression>().value)) {
		state.fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
		state.comparison_type = comparison.GetExpressionType();
		state.constant = comparison.right->Cast<BoundConstantExpression>().value;
		InitializeExecutor(context, expression, state);
		return true;
	}
	if (IsSimpleFilterColumnRef(*comparison.right) && comparison.left->type == ExpressionType::VALUE_CONSTANT &&
	    CanUseConstantComparisonFastPath(*comparison.right, FlipComparisonExpression(comparison.GetExpressionType()),
	                                     comparison.left->Cast<BoundConstantExpression>().value)) {
		state.fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
		state.comparison_type = FlipComparisonExpression(comparison.GetExpressionType());
		state.constant = comparison.left->Cast<BoundConstantExpression>().value;
		InitializeExecutor(context, expression, state);
		return true;
	}
	return false;
}

static bool TryInitializeOperatorState(ClientContext &context, const Expression &expression,
                                       ExpressionFilterState &state) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR) {
		return false;
	}
	auto &op = expression.Cast<BoundOperatorExpression>();
	if (op.children.size() != 1 || !IsSimpleFilterColumnRef(*op.children[0])) {
		return false;
	}
	switch (expression.type) {
	case ExpressionType::OPERATOR_IS_NULL:
		state.fast_path = ExpressionFilterFastPath::IS_NULL;
		InitializeExecutor(context, expression, state);
		return true;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		state.fast_path = ExpressionFilterFastPath::IS_NOT_NULL;
		InitializeExecutor(context, expression, state);
		return true;
	default:
		return false;
	}
}

static bool TryInitializeBloomFilterState(ClientContext &context, const Expression &expression,
                                          const BoundFunctionExpression &function, ExpressionFilterState &state) {
	if (function.function.name != BloomFilterScalarFun::NAME || !function.bind_info) {
		return false;
	}
	auto &bind_data = function.bind_info->Cast<BloomFilterFunctionData>();
	state.fast_path = ExpressionFilterFastPath::BLOOM_FILTER;
	if (bind_data.filter && bind_data.n_vectors_to_check != 0) {
		state.EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
	}
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializeSelectivityOptionalState(ClientContext &context, const Expression &expression,
                                                  const BoundFunctionExpression &function,
                                                  ExpressionFilterState &state) {
	if (function.function.name != SelectivityOptionalFilterScalarFun::NAME || !function.bind_info) {
		return false;
	}
	auto &bind_data = function.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
	if (!bind_data.child_filter_expr) {
		return false;
	}
	state.fast_path = ExpressionFilterFastPath::SELECTIVITY_OPTIONAL;
	state.EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
	state.selectivity_child_state = make_uniq<ExpressionFilterState>(context, *bind_data.child_filter_expr);
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializeOptionalState(ClientContext &context, const Expression &expression,
                                       const BoundFunctionExpression &function, ExpressionFilterState &state) {
	if (function.function.name != OptionalFilterScalarFun::NAME || !function.bind_info) {
		return false;
	}
	state.fast_path = ExpressionFilterFastPath::IS_OPTIONAL;
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializePerfectHashJoinState(ClientContext &context, const Expression &expression,
                                              const BoundFunctionExpression &function, ExpressionFilterState &state) {
	if (function.function.name != PerfectHashJoinScalarFun::NAME || !function.bind_info) {
		return false;
	}
	auto &bind_data = function.bind_info->Cast<PerfectHashJoinFunctionData>();
	if (!bind_data.executor) {
		InitializeExecutor(context, expression, state);
		return true;
	}
	state.fast_path = ExpressionFilterFastPath::PERFECT_HASH_JOIN;
	if (bind_data.n_vectors_to_check != 0) {
		state.EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
	}
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializePrefixRangeState(ClientContext &context, const Expression &expression,
                                          const BoundFunctionExpression &function, ExpressionFilterState &state) {
	if (function.function.name != PrefixRangeScalarFun::NAME || !function.bind_info) {
		return false;
	}
	auto &bind_data = function.bind_info->Cast<PrefixRangeFunctionData>();
	if (!bind_data.filter) {
		InitializeExecutor(context, expression, state);
		return true;
	}
	state.fast_path = ExpressionFilterFastPath::PREFIX_RANGE;
	if (bind_data.n_vectors_to_check != 0) {
		state.EnableSelectivityTracking(bind_data.selectivity_threshold, bind_data.n_vectors_to_check);
	}
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializeDynamicFilterState(ClientContext &context, const Expression &expression,
                                            const BoundFunctionExpression &function, ExpressionFilterState &state) {
	if (function.function.name != DynamicFilterScalarFun::NAME || !function.bind_info ||
	    function.children.size() != 1) {
		return false;
	}
	auto &bind_data = function.bind_info->Cast<DynamicFilterFunctionData>();
	if (!bind_data.filter_data || !HasSupportedFastPathComparisonType(bind_data.filter_data->comparison_type) ||
	    !HasSupportedFastPathPhysicalType(function.children[0]->return_type)) {
		InitializeExecutor(context, expression, state);
		return true;
	}
	state.fast_path = ExpressionFilterFastPath::DYNAMIC_FILTER;
	InitializeExecutor(context, expression, state);
	return true;
}

static bool TryInitializeFunctionState(ClientContext &context, const Expression &expression,
                                       ExpressionFilterState &state) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &function = expression.Cast<BoundFunctionExpression>();
	return TryInitializeBloomFilterState(context, expression, function, state) ||
	       TryInitializeSelectivityOptionalState(context, expression, function, state) ||
	       TryInitializeOptionalState(context, expression, function, state) ||
	       TryInitializePerfectHashJoinState(context, expression, function, state) ||
	       TryInitializePrefixRangeState(context, expression, function, state) ||
	       TryInitializeDynamicFilterState(context, expression, function, state);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	if (TryInitializeConjunctionState(context, expression, *this) ||
	    TryInitializeComparisonState(context, expression, *this) ||
	    TryInitializeOperatorState(context, expression, *this) ||
	    TryInitializeFunctionState(context, expression, *this)) {
		return;
	}
	InitializeExecutor(context, expression, *this);
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
