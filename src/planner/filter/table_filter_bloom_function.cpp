//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_bloom_function.cpp
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
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6;             // a sector is 64 bits, log2(64) = 6
static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
static constexpr idx_t N_BITS = 4;                      // the number of bits to set per hash

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = MaxValue(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = MinValue(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	bitmask = num_sectors - 1;

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	bf = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(bf, num_sectors, 0);

	initialized = true;
}

inline uint64_t GetMask(const hash_t hash) {
	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

	uint64_t mask = 0;

	for (idx_t bit_idx = 8 - N_BITS; bit_idx < 8; bit_idx++) {
		const uint8_t bit_pos = shifts_8[bit_idx];
		mask |= (1ULL << bit_pos);
	}

	return mask;
}

void BloomFilter::InsertHashes(const Vector &hashes_v, idx_t count) const {
	auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	for (idx_t i = 0; i < count; i++) {
		InsertOne(hashes[i]);
	}
}

idx_t BloomFilter::LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, const idx_t count) const {
	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);

	const auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		result_sel.set_index(found_count, i);
		found_count += LookupOne(hashes[i]);
	}
	return found_count;
}

inline void BloomFilter::InsertOne(const hash_t hash) const {
	D_ASSERT(initialized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(hash);
	atomic<uint64_t> &slot = *reinterpret_cast<atomic<uint64_t> *>(&bf[bf_offset]);

	slot.fetch_or(mask, std::memory_order_relaxed);
}

inline bool BloomFilter::LookupOne(const uint64_t hash) const {
	D_ASSERT(initialized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(hash);
	atomic<uint64_t> &slot = *reinterpret_cast<atomic<uint64_t> *>(&bf[bf_offset]);
	auto bf_entry = slot.load(std::memory_order_relaxed);

	return (bf_entry & mask) == mask;
}

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

static idx_t SelectBloomFilter(Vector &input, const BloomFilterFunctionData &func_data, SelectionVector &result_sel,
                               idx_t count) {
	D_ASSERT(func_data.filter);
	Vector hashes(LogicalType::HASH, count);
	VectorOperations::Hash(input, hashes, count);
	hashes.Flatten();

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(input_data);

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

static idx_t BloomFilterSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                               optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<BloomFilterFunctionData>();
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	auto tracking_state = local_state_ptr ? &local_state_ptr->Cast<SelectivityTrackingLocalState>() : nullptr;

	auto count = args.size();
	if (!func_data.filter) {
		return SetAllTrueSelection(count, sel, true_sel, false_sel);
	}
	if (tracking_state && !tracking_state->IsActive()) {
		tracking_state->Update(0, 0);
		return SetAllTrueSelection(count, sel, true_sel, false_sel);
	}

	SelectionVector temp_true(count);
	auto result_true_sel = (!true_sel || (sel && true_sel.get() == sel.get())) ? &temp_true : true_sel.get();
	auto approved_count = SelectBloomFilter(args.data[0], func_data, *result_true_sel, count);
	approved_count = TranslateSelection(count, sel, *result_true_sel, approved_count, true_sel, false_sel);
	if (tracking_state) {
		tracking_state->Update(approved_count, count);
	}
	return approved_count;
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
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, nullptr, TableFilterFunctions::Bind);
	func.SetInitStateCallback(BloomFilterInitLocalState);
	func.SetSelectCallback(BloomFilterSelect);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(BloomFilterScalarFun::FilterPrune);
	func.SetSerializeCallback(TableFilterFunctionSerialize);
	func.SetDeserializeCallback(TableFilterFunctionDeserialize);
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
