//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_bloom_function
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

BloomFilterFunctionData::BloomFilterFunctionData(optional_ptr<BloomFilter> filter_p, bool filters_null_values_p,
                                                 const string &key_column_name_p, const LogicalType &key_type_p,
                                                 float selectivity_threshold_p, idx_t n_vectors_to_check_p)
    : filter(filter_p), filters_null_values(filters_null_values_p), key_column_name(key_column_name_p),
      key_type(key_type_p), selectivity_threshold(selectivity_threshold_p), n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> BloomFilterFunctionData::Copy() const {
	return make_uniq<BloomFilterFunctionData>(filter, filters_null_values, key_column_name, key_type,
	                                          selectivity_threshold, n_vectors_to_check);
}

bool BloomFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<BloomFilterFunctionData>();
	return filter.get() == other.filter.get() && filters_null_values == other.filters_null_values &&
	       key_column_name == other.key_column_name && key_type == other.key_type;
}

idx_t SelectBloomFilter(Vector &input, const BloomFilterFunctionData &func_data, SelectionVector &result_sel,
                        idx_t count) {
	D_ASSERT(func_data.filter);
	Vector hashes(LogicalType::HASH, count);
	VectorOperations::Hash(input, hashes, count);
	hashes.Flatten(count);

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	SelectionVector bloom_sel(count);
	const auto bloom_count = func_data.filter->LookupHashes(hashes, bloom_sel, count);

	idx_t result_count = 0;
	idx_t bloom_idx = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto matched = bloom_idx < bloom_count && bloom_sel.get_index_unsafe(bloom_idx) == i;
		if (matched) {
			bloom_idx++;
		}
		const auto input_idx = input_data.sel->get_index(i);
		bool passed;
		if (!input_data.validity.RowIsValid(input_idx)) {
			passed = !func_data.filters_null_values;
		} else {
			passed = matched;
		}
		if (passed) {
			result_sel.set_index(result_count++, i);
		}
	}
	return result_count;
}

static unique_ptr<FunctionLocalState>
BloomFilterInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<BloomFilterFunctionData>();
	if (!data.filter) {
		return nullptr;
	}
	return InitSelectivityTrackingLocalState(data.n_vectors_to_check, data.selectivity_threshold);
}

static void BloomFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<BloomFilterFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();
	auto &input = args.data[0];

	if (!func_data.filter) {
		SetAllTrue(args, result);
		return;
	}
	ExecuteWithSelectivityTracking(args, result, tracking_state, [&] {
		SelectionVector result_sel(count);
		auto passed = SelectBloomFilter(input, func_data, result_sel, count);
		SelectionToBooleanResult(count, result_sel, passed, result);
		return passed;
	});
}

template <class T>
static FilterPropagateResult TemplatedBloomFilterPrune(const BloomFilter &bf, const BaseStatistics &stats) {
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

	T val = min;
	idx_t hits = 0;
	for (idx_t i = 0; i <= range; i++) {
		hits += bf.LookupOne(Hash(val));
		val += i < range;
	}

	if (hits == 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (hits == range + 1) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ScalarFunction BloomFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, BloomFilterFunction, TableFilterFunctions::Bind);
	func.SetInitStateCallback(BloomFilterInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(BloomFilterScalarFun::FilterPrune);
	func.serialize = TableFilterFunctionSerialize;
	func.deserialize = TableFilterFunctionDeserialize;
	return func;
}

string BloomFilterScalarFun::ToString(const string &column_name, const string &key_column_name) {
	return column_name + " IN BF(" + key_column_name + ")";
}

FilterPropagateResult BloomFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<BloomFilterFunctionData>();
	if (!data.filter || !data.filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	switch (data.key_type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedBloomFilterPrune<uint8_t>(*data.filter, input.stats);
	case PhysicalType::UINT16:
		return TemplatedBloomFilterPrune<uint16_t>(*data.filter, input.stats);
	case PhysicalType::UINT32:
		return TemplatedBloomFilterPrune<uint32_t>(*data.filter, input.stats);
	case PhysicalType::UINT64:
		return TemplatedBloomFilterPrune<uint64_t>(*data.filter, input.stats);
	case PhysicalType::UINT128:
		return TemplatedBloomFilterPrune<uhugeint_t>(*data.filter, input.stats);
	case PhysicalType::INT8:
		return TemplatedBloomFilterPrune<int8_t>(*data.filter, input.stats);
	case PhysicalType::INT16:
		return TemplatedBloomFilterPrune<int16_t>(*data.filter, input.stats);
	case PhysicalType::INT32:
		return TemplatedBloomFilterPrune<int32_t>(*data.filter, input.stats);
	case PhysicalType::INT64:
		return TemplatedBloomFilterPrune<int64_t>(*data.filter, input.stats);
	case PhysicalType::INT128:
		return TemplatedBloomFilterPrune<hugeint_t>(*data.filter, input.stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

ScalarFunction TableFilterBloomFilterFun::GetFunction() {
	return BloomFilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
