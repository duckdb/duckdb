//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/tablefilter_internal_functions
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"
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

bool TableFilterInternalFunctions::IsInternalTableFilterFunction(const string &name) {
	static const char *const INTERNAL_TABLE_FILTER_FUNCTIONS[] = {
	    BloomFilterScalarFun::NAME,     DynamicFilterScalarFun::NAME, OptionalFilterScalarFun::NAME,
	    PerfectHashJoinScalarFun::NAME, PrefixRangeScalarFun::NAME,   SelectivityOptionalFilterScalarFun::NAME};
	for (auto function_name : INTERNAL_TABLE_FILTER_FUNCTIONS) {
		if (name == function_name) {
			return true;
		}
	}
	return false;
}
// LCOV_EXCL_STOP

void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check) {
	static constexpr float MIN_MAX_THRESHOLD = 0.9f;
	static constexpr float BF_THRESHOLD = 0.5f;
	static constexpr float PHJ_THRESHOLD = 0.3f;
	static constexpr float PRF_THRESHOLD = 0.5f;

	static constexpr idx_t MIN_MAX_CHECK_N = 6;
	static constexpr idx_t BF_CHECK_N = 6;
	static constexpr idx_t PHJ_CHECK_N = 6;
	static constexpr idx_t PRF_CHECK_N = 6;

	switch (type) {
	case SelectivityOptionalFilterType::MIN_MAX:
		selectivity_threshold = MIN_MAX_THRESHOLD;
		n_vectors_to_check = MIN_MAX_CHECK_N;
		return;
	case SelectivityOptionalFilterType::BF:
		selectivity_threshold = BF_THRESHOLD;
		n_vectors_to_check = BF_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PHJ:
		selectivity_threshold = PHJ_THRESHOLD;
		n_vectors_to_check = PHJ_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PRF:
		selectivity_threshold = PRF_THRESHOLD;
		n_vectors_to_check = PRF_CHECK_N;
		return;
	default:
		throw NotImplementedException("GetThresholdAndVectorsToCheck");
	}
}

static void SelectionToBooleanResult(idx_t count, const SelectionVector &sel, idx_t sel_count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}
	for (idx_t i = 0; i < sel_count; i++) {
		result_data[sel.get_index(i)] = true;
	}
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

idx_t SelectPerfectHashJoin(Vector &input, const PerfectHashJoinFunctionData &func_data, SelectionVector &result_sel,
                            idx_t count) {
	D_ASSERT(func_data.executor);
	idx_t approved_count = 0;
	func_data.executor->FillSelectionVectorSwitchProbe(input, count, result_sel, approved_count, nullptr);
	return approved_count;
}

idx_t SelectPrefixRange(Vector &input, const PrefixRangeFunctionData &func_data, SelectionVector &result_sel,
                        idx_t count) {
	D_ASSERT(func_data.filter);
	return func_data.filter->LookupKeys(input, result_sel, count);
}

static void TableFilterInternalSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                         const ScalarFunction &function) {
	// Runtime state cannot be serialized - write nothing
}

static unique_ptr<FunctionData> TableFilterInternalDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	auto key_type = function.arguments.empty() ? LogicalType::ANY : function.arguments[0];
	if (function.name == BloomFilterScalarFun::NAME) {
		return make_uniq<BloomFilterFunctionData>(nullptr, false, string(), key_type, 0.0f, idx_t(0));
	}
	if (function.name == PerfectHashJoinScalarFun::NAME) {
		return make_uniq<PerfectHashJoinFunctionData>(nullptr, string(), 0.0f, idx_t(0));
	}
	if (function.name == PrefixRangeScalarFun::NAME) {
		return make_uniq<PrefixRangeFunctionData>(nullptr, string(), key_type, 0.0f, idx_t(0));
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
	SelectivityTrackingLocalState(idx_t n_vectors_to_check_p, float selectivity_threshold_p) : stats() {
		stats.Enable(selectivity_threshold_p, n_vectors_to_check_p);
	}

	void Update(idx_t accepted, idx_t processed) {
		stats.Update(accepted, processed);
	}

	bool IsActive() const {
		return stats.IsActive();
	}

	SelectivityTrackingState stats;
};

static unique_ptr<FunctionLocalState> InitSelectivityTrackingLocalState(idx_t n_vectors_to_check,
                                                                        float selectivity_threshold) {
	if (n_vectors_to_check == 0) {
		return nullptr;
	}
	return make_uniq<SelectivityTrackingLocalState>(n_vectors_to_check, selectivity_threshold);
}

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
	if (!data.filter) {
		return nullptr;
	}
	return InitSelectivityTrackingLocalState(data.n_vectors_to_check, data.selectivity_threshold);
}

static void SetAllTrue(DataChunk &args, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
}

template <class TRACKING_STATE, class EXECUTOR>
static void ExecuteWithSelectivityTracking(DataChunk &args, Vector &result, TRACKING_STATE *tracking_state,
                                           EXECUTOR &&execute) {
	if (tracking_state && !tracking_state->IsActive()) {
		SetAllTrue(args, result);
		tracking_state->Update(0, 0);
		return;
	}
	auto approved_count = execute();
	if (tracking_state) {
		tracking_state->Update(approved_count, args.size());
	}
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

ScalarFunction BloomFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, BloomFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(BloomFilterInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(BloomFilterScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
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

//===----------------------------------------------------------------------===//
// Perfect Hash Join Scalar Function
//===----------------------------------------------------------------------===//

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
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, PerfectHashJoinFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(PerfectHashJoinInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(PerfectHashJoinScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
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

//===----------------------------------------------------------------------===//
// Prefix Range Scalar Function
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionLocalState>
PrefixRangeInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &data = bind_data->Cast<PrefixRangeFunctionData>();
	if (!data.filter) {
		return nullptr;
	}
	return InitSelectivityTrackingLocalState(data.n_vectors_to_check, data.selectivity_threshold);
}

static void PrefixRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<PrefixRangeFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();

	if (!func_data.filter || !func_data.filter->IsInitialized()) {
		SetAllTrue(args, result);
		return;
	}

	auto &input = args.data[0];
	ExecuteWithSelectivityTracking(args, result, tracking_state, [&] {
		SelectionVector lookup_sel(count);
		auto found_count = SelectPrefixRange(input, func_data, lookup_sel, count);
		SelectionToBooleanResult(count, lookup_sel, found_count, result);
		return found_count;
	});
}

ScalarFunction PrefixRangeScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, PrefixRangeFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(PrefixRangeInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(PrefixRangeScalarFun::FilterPrune);
	func.serialize = TableFilterInternalSerialize;
	func.deserialize = TableFilterInternalDeserialize;
	return func;
}

string PrefixRangeScalarFun::ToString(const string &column_name, const string &key_column_name) {
	return column_name + " IN PRF(" + key_column_name + ")";
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

ScalarFunction DynamicFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, DynamicFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
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

string DynamicFilterScalarFun::ToString(const string &column_name, bool has_filter_data) {
	if (has_filter_data) {
		return "Dynamic Filter (" + column_name + ")";
	}
	return "Dynamic Filter";
}

static string OptionalFilterToString(const string &child_filter_string) {
	if (child_filter_string.empty()) {
		return "optional";
	}
	return "optional: " + child_filter_string;
}

//===----------------------------------------------------------------------===//
// Optional Filter Scalar Function
//===----------------------------------------------------------------------===//

static void OptionalFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Optional filter always returns TRUE - the filter is for statistics pruning only
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
}

ScalarFunction OptionalFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, OptionalFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
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

string OptionalFilterScalarFun::ToString(const string &child_filter_string) {
	return OptionalFilterToString(child_filter_string);
}

//===----------------------------------------------------------------------===//
// Selectivity-Optional Filter Scalar Function
//===----------------------------------------------------------------------===//

struct SelectivityOptionalFilterLocalState : public FunctionLocalState {
	SelectivityOptionalFilterLocalState(ClientContext &context, const Expression &child_filter_expr,
	                                    idx_t n_vectors_to_check, float selectivity_threshold)
	    : stats(n_vectors_to_check, selectivity_threshold), executor(context, child_filter_expr) {
	}

	bool IsActive() const {
		return stats.IsActive();
	}
	void Update(idx_t accepted, idx_t processed) {
		stats.Update(accepted, processed);
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
	ExecuteWithSelectivityTracking(args, result, &local_state, [&] {
		SelectionVector child_sel(count);
		auto approved_count = local_state.executor.SelectExpression(args, child_sel);
		SelectionToBooleanResult(count, child_sel, approved_count, result);
		return approved_count;
	});
}

ScalarFunction SelectivityOptionalFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, SelectivityOptionalFilterFunction,
	                    TableFilterInternalFunctions::Bind);
	func.SetInitStateCallback(SelectivityOptionalFilterInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
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

string SelectivityOptionalFilterScalarFun::ToString(const string &child_filter_string) {
	return OptionalFilterToString(child_filter_string);
}

ScalarFunction InternalTableFilterBloomFilterFun::GetFunction() {
	return BloomFilterScalarFun::GetFunction(LogicalType::ANY);
}

ScalarFunction InternalTableFilterDynamicFun::GetFunction() {
	return DynamicFilterScalarFun::GetFunction(LogicalType::ANY);
}

ScalarFunction InternalTableFilterOptionalFun::GetFunction() {
	return OptionalFilterScalarFun::GetFunction(LogicalType::ANY);
}

ScalarFunction InternalTableFilterPerfectHashJoinFun::GetFunction() {
	return PerfectHashJoinScalarFun::GetFunction(LogicalType::ANY);
}

ScalarFunction InternalTableFilterPrefixRangeFun::GetFunction() {
	return PrefixRangeScalarFun::GetFunction(LogicalType::ANY);
}

ScalarFunction InternalTableFilterSelectivityOptionalFun::GetFunction() {
	return SelectivityOptionalFilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
