//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/tablefilter_internal_functions
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/prefix_range_filter.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// TableFilterInternalFunctions
//===----------------------------------------------------------------------===//

// LCOV_EXCL_START
unique_ptr<FunctionData> TableFilterInternalFunctions::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	throw BinderException("Table filter functions are for internal use only!");
}
// LCOV_EXCL_STOP

static void TableFilterInternalSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                         const ScalarFunction &function) {
	// Runtime state cannot be serialized - write nothing
}

static unique_ptr<FunctionData> TableFilterInternalDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	auto key_type = function.arguments.empty() ? LogicalType::ANY : function.arguments[0];
	if (function.name == BloomFilterScalarFun::NAME) {
		return make_uniq<BloomFilterFunctionData>(nullptr, false, string(), key_type, 0.0f, 0);
	}
	if (function.name == PerfectHashJoinScalarFun::NAME) {
		return make_uniq<PerfectHashJoinFunctionData>(nullptr, string(), 0.0f, 0);
	}
	if (function.name == PrefixRangeScalarFun::NAME) {
		return make_uniq<PrefixRangeFunctionData>(nullptr, string(), key_type, 0.0f, 0);
	}
	if (function.name == DynamicFilterScalarFun::NAME) {
		return make_uniq<DynamicFilterFunctionData>(nullptr);
	}
	throw InternalException("Unsupported internal tablefilter function \"%s\" during deserialization", function.name);
}

static void OptionalFilterSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                    const ScalarFunction &function) {
	if (!bind_data) {
		return;
	}
	auto &data = bind_data->Cast<OptionalFilterFunctionData>();
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr", data.child_filter_expr);
}

static unique_ptr<FunctionData> OptionalFilterDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	auto child_filter_expr = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr");
	return make_uniq<OptionalFilterFunctionData>(std::move(child_filter_expr));
}

static void SelectivityOptionalFilterSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                               const ScalarFunction &function) {
	if (!bind_data) {
		return;
	}
	auto &data = bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr", data.child_filter_expr);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", data.selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", data.n_vectors_to_check);
}

static unique_ptr<FunctionData> SelectivityOptionalFilterDeserialize(Deserializer &deserializer,
                                                                     ScalarFunction &function) {
	auto child_filter_expr = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr");
	auto selectivity_threshold =
	    deserializer.ReadPropertyWithExplicitDefault<float>(201, "selectivity_threshold", 0.5f);
	auto n_vectors_to_check = deserializer.ReadPropertyWithExplicitDefault<idx_t>(202, "n_vectors_to_check", idx_t(6));
	return make_uniq<SelectivityOptionalFilterFunctionData>(std::move(child_filter_expr), selectivity_threshold,
	                                                        n_vectors_to_check);
}

//===----------------------------------------------------------------------===//
// Selectivity tracking local state
//===----------------------------------------------------------------------===//

struct SelectivityTrackingLocalState : public FunctionLocalState {
	enum class FilterStatus { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	idx_t n_vectors_to_check;
	float selectivity_threshold;
	idx_t tuples_accepted = 0;
	idx_t tuples_processed = 0;
	idx_t vectors_processed = 0;
	FilterStatus status = FilterStatus::ACTIVE;
	idx_t pause_multiplier = 0;

	SelectivityTrackingLocalState(idx_t n_vectors_to_check_p, float selectivity_threshold_p)
	    : n_vectors_to_check(n_vectors_to_check_p), selectivity_threshold(selectivity_threshold_p) {
	}

	void Update(idx_t accepted, idx_t processed) {
		vectors_processed++;
		tuples_accepted += accepted;
		tuples_processed += processed;

		static constexpr idx_t VECTOR_PAUSE = 10;
		D_ASSERT(n_vectors_to_check < VECTOR_PAUSE);
		if (vectors_processed == MaxValue<idx_t>(pause_multiplier, 1) * VECTOR_PAUSE) {
			vectors_processed = 0;
			tuples_accepted = 0;
			tuples_processed = 0;
			status = FilterStatus::ACTIVE;
		} else if (vectors_processed >= n_vectors_to_check) {
			if (GetSelectivity() >= selectivity_threshold) {
				status = FilterStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY;
				pause_multiplier++;
			} else {
				pause_multiplier = 0;
			}
		}
	}

	bool IsActive() const {
		return status == FilterStatus::ACTIVE;
	}

	double GetSelectivity() const {
		if (tuples_processed == 0) {
			return 0.0;
		}
		return static_cast<double>(tuples_accepted) / static_cast<double>(tuples_processed);
	}
};

//===----------------------------------------------------------------------===//
// BloomFilterFunctionData
//===----------------------------------------------------------------------===//

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

//===----------------------------------------------------------------------===//
// PerfectHashJoinFunctionData
//===----------------------------------------------------------------------===//

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

//===----------------------------------------------------------------------===//
// PrefixRangeFunctionData
//===----------------------------------------------------------------------===//

PrefixRangeFunctionData::PrefixRangeFunctionData(optional_ptr<PrefixRangeFilter> filter_p,
                                                 const string &key_column_name_p, const LogicalType &key_type_p,
                                                 float selectivity_threshold_p, idx_t n_vectors_to_check_p)
    : filter(filter_p), key_column_name(key_column_name_p), key_type(key_type_p),
      selectivity_threshold(selectivity_threshold_p), n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> PrefixRangeFunctionData::Copy() const {
	return make_uniq<PrefixRangeFunctionData>(filter, key_column_name, key_type, selectivity_threshold,
	                                          n_vectors_to_check);
}

bool PrefixRangeFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<PrefixRangeFunctionData>();
	return filter.get() == other.filter.get() && key_column_name == other.key_column_name && key_type == other.key_type;
}

//===----------------------------------------------------------------------===//
// DynamicFilterFunctionData
//===----------------------------------------------------------------------===//

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

//===----------------------------------------------------------------------===//
// OptionalFilterFunctionData
//===----------------------------------------------------------------------===//

OptionalFilterFunctionData::OptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p)
    : child_filter_expr(std::move(child_filter_expr_p)) {
}

unique_ptr<FunctionData> OptionalFilterFunctionData::Copy() const {
	return make_uniq<OptionalFilterFunctionData>(child_filter_expr ? child_filter_expr->Copy() : nullptr);
}

bool OptionalFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<OptionalFilterFunctionData>();
	if (!child_filter_expr && !other.child_filter_expr) {
		return true;
	}
	if (!child_filter_expr || !other.child_filter_expr) {
		return false;
	}
	return child_filter_expr->Equals(*other.child_filter_expr);
}

//===----------------------------------------------------------------------===//
// SelectivityOptionalFilterFunctionData
//===----------------------------------------------------------------------===//

SelectivityOptionalFilterFunctionData::SelectivityOptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p,
                                                                             float selectivity_threshold_p,
                                                                             idx_t n_vectors_to_check_p)
    : child_filter_expr(std::move(child_filter_expr_p)), selectivity_threshold(selectivity_threshold_p),
      n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> SelectivityOptionalFilterFunctionData::Copy() const {
	return make_uniq<SelectivityOptionalFilterFunctionData>(child_filter_expr ? child_filter_expr->Copy() : nullptr,
	                                                        selectivity_threshold, n_vectors_to_check);
}

bool SelectivityOptionalFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<SelectivityOptionalFilterFunctionData>();
	if (selectivity_threshold != other.selectivity_threshold || n_vectors_to_check != other.n_vectors_to_check) {
		return false;
	}
	if (!child_filter_expr && !other.child_filter_expr) {
		return true;
	}
	if (!child_filter_expr || !other.child_filter_expr) {
		return false;
	}
	return child_filter_expr->Equals(*other.child_filter_expr);
}

//===----------------------------------------------------------------------===//
// Bloom Filter Scalar Function
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionLocalState>
BloomFilterInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<BloomFilterFunctionData>();
	if (!data.filter || data.n_vectors_to_check == 0) {
		return nullptr;
	}
	return make_uniq<SelectivityTrackingLocalState>(data.n_vectors_to_check, data.selectivity_threshold);
}

static void SetAllTrue(DataChunk &args, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
}

static void BloomFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<BloomFilterFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();
	auto &input = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if (!func_data.filter) {
		SetAllTrue(args, result);
		return;
	}
	if (tracking_state && !tracking_state->IsActive()) {
		// Paused due to high selectivity - return TRUE for all
		for (idx_t i = 0; i < count; i++) {
			result_data[i] = true;
		}
		tracking_state->Update(0, 0);
		return;
	}

	// Hash the input vector and look up in the bloom filter
	Vector hashes(LogicalType::HASH, count);
	VectorOperations::Hash(input, hashes, count);
	hashes.Flatten(count);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	idx_t passed = 0;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(input_idx)) {
			result_data[i] = !func_data.filters_null_values;
			if (result_data[i]) {
				passed++;
			}
		} else {
			result_data[i] = func_data.filter->LookupOne(hash_data[i]);
			if (result_data[i]) {
				passed++;
			}
		}
	}

	if (tracking_state) {
		tracking_state->Update(passed, count);
	}
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
		val += i < range; // Avoids potential signed integer overflow on the last iteration
	}

	if (hits == 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (hits == range + 1) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ScalarFunction BloomFilterScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction BloomFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, BloomFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(BloomFilterInitLocalState);
	func.SetFilterPruneCallback(BloomFilterScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
	return func;
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

//===----------------------------------------------------------------------===//
// Perfect Hash Join Scalar Function
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionLocalState>
PerfectHashJoinInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<PerfectHashJoinFunctionData>();
	if (!data.executor || data.n_vectors_to_check == 0) {
		return nullptr;
	}
	return make_uniq<SelectivityTrackingLocalState>(data.n_vectors_to_check, data.selectivity_threshold);
}

static void PerfectHashJoinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<PerfectHashJoinFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if ((tracking_state && !tracking_state->IsActive()) || !func_data.executor) {
		for (idx_t i = 0; i < count; i++) {
			result_data[i] = true;
		}
		if (tracking_state) {
			tracking_state->Update(0, 0);
		}
		return;
	}

	// Probe the perfect hash join using FillSelectionVectorSwitchProbe
	auto &input = args.data[0];
	SelectionVector probe_sel(count);
	idx_t approved_count = 0;
	func_data.executor->FillSelectionVectorSwitchProbe(input, count, probe_sel, approved_count, nullptr);

	// Convert selection vector to boolean result
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}
	for (idx_t i = 0; i < approved_count; i++) {
		result_data[probe_sel.get_index(i)] = true;
	}

	if (tracking_state) {
		tracking_state->Update(approved_count, count);
	}
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

ScalarFunction PerfectHashJoinScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction PerfectHashJoinScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, PerfectHashJoinFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(PerfectHashJoinInitLocalState);
	func.SetFilterPruneCallback(PerfectHashJoinScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
	return func;
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

//===----------------------------------------------------------------------===//
// Prefix Range Scalar Function
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionLocalState>
PrefixRangeInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<PrefixRangeFunctionData>();
	if (!data.filter || data.n_vectors_to_check == 0) {
		return nullptr;
	}
	return make_uniq<SelectivityTrackingLocalState>(data.n_vectors_to_check, data.selectivity_threshold);
}

static void PrefixRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<PrefixRangeFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if ((tracking_state && !tracking_state->IsActive()) || !func_data.filter || !func_data.filter->IsInitialized()) {
		for (idx_t i = 0; i < count; i++) {
			result_data[i] = true;
		}
		if (tracking_state) {
			tracking_state->Update(0, 0);
		}
		return;
	}

	// Lookup keys in the prefix range filter
	auto &input = args.data[0];
	SelectionVector lookup_sel(count);
	idx_t found_count = func_data.filter->LookupKeys(input, lookup_sel, count);

	// Convert selection vector to boolean result
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}
	for (idx_t i = 0; i < found_count; i++) {
		result_data[lookup_sel.get_index(i)] = true;
	}

	if (tracking_state) {
		tracking_state->Update(found_count, count);
	}
}

ScalarFunction PrefixRangeScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction PrefixRangeScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, PrefixRangeFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(PrefixRangeInitLocalState);
	func.SetFilterPruneCallback(PrefixRangeScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
	return func;
}

FilterPropagateResult PrefixRangeScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<PrefixRangeFunctionData>();
	if (!data.filter || !data.filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (input.stats.GetStatsType() != StatisticsType::NUMERIC_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!NumericStats::HasMinMax(input.stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::Min(input.stats);
	const auto max = NumericStats::Max(input.stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	return data.filter->LookupRange(min, max);
}

//===----------------------------------------------------------------------===//
// Dynamic Filter Scalar Function
//===----------------------------------------------------------------------===//

static void DynamicFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<DynamicFilterFunctionData>();
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if (!func_data.filter_data || !func_data.filter_data->initialized.load()) {
		// Not initialized yet - return TRUE for all
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

ScalarFunction DynamicFilterScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction DynamicFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, DynamicFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetFilterPruneCallback(DynamicFilterScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
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

//===----------------------------------------------------------------------===//
// Optional Filter Scalar Function
//===----------------------------------------------------------------------===//

static void OptionalFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Optional filter always returns TRUE - the filter is for statistics pruning only
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
}

ScalarFunction OptionalFilterScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction OptionalFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, OptionalFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetFilterPruneCallback(OptionalFilterScalarFun::FilterPrune);
	func.serialize = OptionalFilterSerialize;
	func.deserialize = OptionalFilterDeserialize;
	return func;
}

FilterPropagateResult OptionalFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<OptionalFilterFunctionData>();
	if (!data.child_filter_expr) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// Delegate to the child expression's statistics check
	return ExpressionFilter::CheckExpressionStatistics(*data.child_filter_expr, input.stats);
}

//===----------------------------------------------------------------------===//
// Selectivity-Optional Filter Scalar Function
//===----------------------------------------------------------------------===//

struct SelectivityOptionalFilterLocalState : public FunctionLocalState {
	SelectivityOptionalFilterLocalState(ClientContext &context, const Expression &child_filter_expr,
	                                    idx_t n_vectors_to_check, float selectivity_threshold)
	    : stats(n_vectors_to_check, selectivity_threshold), executor(context, child_filter_expr) {
	}

	SelectivityTrackingLocalState stats;
	ExpressionExecutor executor;
};

static unique_ptr<FunctionLocalState> SelectivityOptionalFilterInitLocalState(ExpressionState &state,
                                                                              const BoundFunctionExpression &expr,
                                                                              FunctionData *bind_data) {
	auto &data = bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	if (!data.child_filter_expr) {
		return nullptr;
	}
	return make_uniq<SelectivityOptionalFilterLocalState>(state.GetContext(), *data.child_filter_expr,
	                                                      data.n_vectors_to_check, data.selectivity_threshold);
}

static void SelectivityOptionalFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	if (!local_state_ptr) {
		SetAllTrue(args, result);
		return;
	}
	auto &local_state = local_state_ptr->Cast<SelectivityOptionalFilterLocalState>();
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	if (!local_state.stats.IsActive()) {
		for (idx_t i = 0; i < count; i++) {
			result_data[i] = true;
		}
		local_state.stats.Update(0, 0);
		return;
	}

	SelectionVector child_sel(count);
	auto approved_count = local_state.executor.SelectExpression(args, child_sel);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}
	for (idx_t i = 0; i < approved_count; i++) {
		result_data[child_sel.get_index(i)] = true;
	}
	local_state.stats.Update(approved_count, count);
}

ScalarFunction SelectivityOptionalFilterScalarFun::GetFunction() {
	return GetFunction(LogicalType::ANY);
}

ScalarFunction SelectivityOptionalFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, SelectivityOptionalFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(SelectivityOptionalFilterInitLocalState);
	func.SetFilterPruneCallback(SelectivityOptionalFilterScalarFun::FilterPrune);
	func.serialize = SelectivityOptionalFilterSerialize;
	func.deserialize = SelectivityOptionalFilterDeserialize;
	return func;
}

FilterPropagateResult SelectivityOptionalFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	if (!data.child_filter_expr) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return ExpressionFilter::CheckExpressionStatistics(*data.child_filter_expr, input.stats);
}

} // namespace duckdb
