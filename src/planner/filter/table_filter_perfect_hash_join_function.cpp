//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_perfect_hash_join_function
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "table_filter_function_helpers.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

PerfectHashJoinFunctionData::PerfectHashJoinFunctionData(optional_ptr<const PerfectHashJoinExecutor> executor_p,
                                                         const string &key_column_name_p, float selectivity_threshold_p,
                                                         idx_t n_vectors_to_check_p)
    : executor(executor_p), key_column_name(key_column_name_p), selectivity_threshold(selectivity_threshold_p),
      n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> PerfectHashJoinFunctionData::Copy() const {
	return make_uniq<PerfectHashJoinFunctionData>(executor, key_column_name, selectivity_threshold, n_vectors_to_check);
}

bool PerfectHashJoinFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<PerfectHashJoinFunctionData>();
	return executor.get() == other.executor.get() && key_column_name == other.key_column_name;
}

idx_t SelectPerfectHashJoin(Vector &input, const PerfectHashJoinFunctionData &func_data, SelectionVector &result_sel,
                            idx_t count) {
	D_ASSERT(func_data.executor);
	idx_t approved_count = 0;
	func_data.executor->FillSelectionVectorSwitchProbe(input, count, result_sel, approved_count, nullptr);
	return approved_count;
}

static unique_ptr<FunctionLocalState>
PerfectHashJoinInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<PerfectHashJoinFunctionData>();
	if (!data.executor) {
		return nullptr;
	}
	return InitSelectivityTrackingLocalState(data.n_vectors_to_check, data.selectivity_threshold);
}

static void PerfectHashJoinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<PerfectHashJoinFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();

	if (!func_data.executor) {
		SetAllTrue(args, result);
		return;
	}

	auto &input = args.data[0];
	ExecuteWithSelectivityTracking(args, result, tracking_state, [&] {
		SelectionVector probe_sel(count);
		auto approved_count = SelectPerfectHashJoin(input, func_data, probe_sel, count);
		SelectionToBooleanResult(count, probe_sel, approved_count, result);
		return approved_count;
	});
}

template <class T>
static FilterPropagateResult TemplatedPerfectHashJoinPrune(const PerfectHashJoinExecutor &executor,
                                                           const BaseStatistics &stats) {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::GetMin<T>(stats);
	const auto max = NumericStats::GetMax<T>(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	T range_typed;
	idx_t range;
	if (!TrySubtractOperator::Operation(max, min, range_typed) || !TryCast::Operation(range_typed, range) ||
	    range >= DEFAULT_STANDARD_VECTOR_SIZE) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	Vector range_vec(stats.GetType(), DEFAULT_STANDARD_VECTOR_SIZE);
	auto range_data = FlatVector::GetData<T>(range_vec);
	T val = min;
	for (; val < max; val += 1) {
		*range_data++ = val;
	}
	*range_data = val;

	const auto total_count = NumericCast<idx_t>(range_typed) + 1;
	idx_t approved_tuple_count = 0;
	SelectionVector probe_sel(total_count);
	executor.FillSelectionVectorSwitchProbe(range_vec, total_count, probe_sel, approved_tuple_count, nullptr);

	if (approved_tuple_count == 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (approved_tuple_count == total_count) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ScalarFunction PerfectHashJoinScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, PerfectHashJoinFunction, TableFilterFunctions::Bind);
	func.SetInitStateCallback(PerfectHashJoinInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(PerfectHashJoinScalarFun::FilterPrune);
	func.serialize = TableFilterFunctionSerialize;
	func.deserialize = TableFilterFunctionDeserialize;
	return func;
}

string PerfectHashJoinScalarFun::ToString(const string &column_name, const string &key_column_name) {
	return column_name + " IN PHJ(" + key_column_name + ")";
}

FilterPropagateResult PerfectHashJoinScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<PerfectHashJoinFunctionData>();
	if (!data.executor) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	switch (input.stats.GetType().InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedPerfectHashJoinPrune<uint8_t>(*data.executor, input.stats);
	case PhysicalType::UINT16:
		return TemplatedPerfectHashJoinPrune<uint16_t>(*data.executor, input.stats);
	case PhysicalType::UINT32:
		return TemplatedPerfectHashJoinPrune<uint32_t>(*data.executor, input.stats);
	case PhysicalType::UINT64:
		return TemplatedPerfectHashJoinPrune<uint64_t>(*data.executor, input.stats);
	case PhysicalType::UINT128:
		return TemplatedPerfectHashJoinPrune<uhugeint_t>(*data.executor, input.stats);
	case PhysicalType::INT8:
		return TemplatedPerfectHashJoinPrune<int8_t>(*data.executor, input.stats);
	case PhysicalType::INT16:
		return TemplatedPerfectHashJoinPrune<int16_t>(*data.executor, input.stats);
	case PhysicalType::INT32:
		return TemplatedPerfectHashJoinPrune<int32_t>(*data.executor, input.stats);
	case PhysicalType::INT64:
		return TemplatedPerfectHashJoinPrune<int64_t>(*data.executor, input.stats);
	case PhysicalType::INT128:
		return TemplatedPerfectHashJoinPrune<hugeint_t>(*data.executor, input.stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

ScalarFunction TableFilterPerfectHashJoinFun::GetFunction() {
	return PerfectHashJoinScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
