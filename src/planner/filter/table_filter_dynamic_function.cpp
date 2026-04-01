//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_dynamic_function
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "table_filter_function_helpers.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

DynamicFilterData::DynamicFilterData(ExpressionType comparison_type_p, Value constant_p)
    : comparison_type(comparison_type_p), constant(std::move(constant_p)) {
}

bool DynamicFilterData::CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return ValueOperations::Equals(value, constant);
	case ExpressionType::COMPARE_NOTEQUAL:
		return ValueOperations::NotEquals(value, constant);
	case ExpressionType::COMPARE_GREATERTHAN:
		return ValueOperations::GreaterThan(value, constant);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ValueOperations::GreaterThanEquals(value, constant);
	case ExpressionType::COMPARE_LESSTHAN:
		return ValueOperations::LessThan(value, constant);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ValueOperations::LessThanEquals(value, constant);
	default:
		throw InternalException("unknown comparison type for DynamicFilter: " + EnumUtil::ToString(comparison_type));
	}
}

FilterPropagateResult DynamicFilterData::CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
                                                         const Value &constant) {
	auto col_ref = make_uniq<BoundReferenceExpression>(stats.GetType(), storage_t(0));
	auto bound_constant = make_uniq<BoundConstantExpression>(constant);
	auto expr = make_uniq<BoundComparisonExpression>(comparison_type, std::move(col_ref), std::move(bound_constant));
	return ExpressionFilter::CheckExpressionStatistics(*expr, stats);
}

DynamicFilterFunctionData::DynamicFilterFunctionData(shared_ptr<DynamicFilterData> filter_data_p)
    : filter_data(std::move(filter_data_p)) {
}

unique_ptr<FunctionData> DynamicFilterFunctionData::Copy() const {
	return make_uniq<DynamicFilterFunctionData>(filter_data);
}

bool DynamicFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DynamicFilterFunctionData>();
	return filter_data.get() == other.filter_data.get();
}

void DynamicFilterData::SetValue(Value val) {
	if (val.IsNull()) {
		return;
	}
	lock_guard<mutex> l(lock);
	constant = std::move(val);
	initialized = true;
}

void DynamicFilterData::Reset() {
	lock_guard<mutex> l(lock);
	initialized = false;
}

static void DynamicFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<DynamicFilterFunctionData>();
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if (!func_data.filter_data || !func_data.filter_data->initialized.load()) {
		for (idx_t i = 0; i < count; i++) {
			result_data[i] = true;
		}
		return;
	}

	ExpressionType comparison_type;
	Value constant;
	{
		lock_guard<mutex> l(func_data.filter_data->lock);
		comparison_type = func_data.filter_data->comparison_type;
		constant = func_data.filter_data->constant;
	}
	auto &input = args.data[0];

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			result_data[i] = false;
		} else {
			auto val = input.GetValue(idx);
			result_data[i] = DynamicFilterData::CompareValue(comparison_type, constant, val);
		}
	}
}

ScalarFunction DynamicFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, DynamicFilterFunction, TableFilterFunctions::Bind);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(DynamicFilterScalarFun::FilterPrune);
	func.serialize = TableFilterFunctionSerialize;
	func.deserialize = TableFilterFunctionDeserialize;
	return func;
}

FilterPropagateResult DynamicFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<DynamicFilterFunctionData>();
	if (!data.filter_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	lock_guard<mutex> l(data.filter_data->lock);
	if (!data.filter_data->initialized) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return DynamicFilterData::CheckStatistics(input.stats, data.filter_data->comparison_type,
	                                          data.filter_data->constant);
}

string DynamicFilterScalarFun::ToString(const string &column_name, bool has_filter_data) {
	if (has_filter_data) {
		return "Dynamic Filter (" + column_name + ")";
	}
	return "Dynamic Filter";
}

ScalarFunction TableFilterDynamicFun::GetFunction() {
	return DynamicFilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
