//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_dynamic_function.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

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

static idx_t SelectDynamicFilter(Vector &input, ExpressionType comparison_type, const Value &constant,
                                 SelectionVector &result_sel, idx_t count) {
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	idx_t approved_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto value = input.GetValue(idx);
		if (!DynamicFilterData::CompareValue(comparison_type, constant, value)) {
			continue;
		}
		result_sel.set_index(approved_count++, i);
	}
	return approved_count;
}

static idx_t DynamicFilterSelect(DataChunk &args, ExpressionState &state, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<DynamicFilterFunctionData>();
	auto count = args.size();
	if (!func_data.filter_data || !func_data.filter_data->initialized.load()) {
		return SetAllTrueSelection(count, true_sel, false_sel);
	}

	ExpressionType comparison_type;
	Value constant;
	{
		lock_guard<mutex> lock(func_data.filter_data->lock);
		comparison_type = func_data.filter_data->comparison_type;
		constant = func_data.filter_data->constant;
	}

	SelectionVector temp_true(count);
	auto result_true_sel = true_sel ? true_sel : &temp_true;
	auto approved_count = SelectDynamicFilter(args.data[0], comparison_type, constant, *result_true_sel, count);
	if (false_sel) {
		FillSelectionInversion(count, *result_true_sel, approved_count, false_sel);
	}
	return approved_count;
}

ScalarFunction DynamicFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, nullptr, TableFilterFunctions::Bind);
	func.SetSelectCallback(DynamicFilterSelect);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(DynamicFilterScalarFun::FilterPrune);
	func.SetSerializeCallback(TableFilterFunctionSerialize);
	func.SetDeserializeCallback(TableFilterFunctionDeserialize);
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
	lock_guard<mutex> lock(data.filter_data->lock);
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
