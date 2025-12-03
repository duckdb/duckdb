#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6;             // a sector is 64 bits, log2(64) = 6
static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
static constexpr idx_t N_BITS = 4;                      // the number of bits to set per hash

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
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
	std::atomic<uint64_t> &slot = *reinterpret_cast<std::atomic<uint64_t> *>(&bf[bf_offset]);

	slot.fetch_or(mask, std::memory_order_relaxed);
}

inline bool BloomFilter::LookupOne(const uint64_t hash) const {
	D_ASSERT(initialized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(hash);

	return (bf[bf_offset] & mask) == mask;
}

string BFTableFilter::ToString(const string &column_name) const {
	return column_name + " IN BF(" + key_column_name + ")";
}

void BFTableFilter::HashInternal(Vector &keys_v, const SelectionVector &sel, const idx_t approved_count,
                                 BFTableFilterState &state) {
	if (sel.IsSet()) {
		state.keys_sliced_v.Slice(keys_v, sel, approved_count);
		VectorOperations::Hash(state.keys_sliced_v, state.hashes_v, approved_count);
	} else {
		VectorOperations::Hash(keys_v, state.hashes_v, approved_count);
	}
}

idx_t BFTableFilter::Filter(Vector &keys_v, SelectionVector &sel, idx_t &approved_tuple_count,
                            BFTableFilterState &state) const {
	if (state.current_capacity < approved_tuple_count) {
		state.hashes_v.Initialize(false, approved_tuple_count);
		state.bf_sel.Initialize(approved_tuple_count);
		state.current_capacity = approved_tuple_count;
	}

	HashInternal(keys_v, sel, approved_tuple_count, state);

	idx_t found_count;
	if (state.hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const auto constant_hash = *ConstantVector::GetData<hash_t>(state.hashes_v);
		const bool found = this->filter.LookupOne(constant_hash);
		found_count = found ? approved_tuple_count : 0;
	} else {
		state.hashes_v.Flatten(approved_tuple_count);
		found_count = this->filter.LookupHashes(state.hashes_v, state.bf_sel, approved_tuple_count);
	}

	// all the elements have been found, we don't need to translate anything
	if (found_count == approved_tuple_count) {
		return approved_tuple_count;
	}

	if (sel.IsSet()) {
		for (idx_t idx = 0; idx < found_count; idx++) {
			const idx_t flat_sel_idx = state.bf_sel.get_index(idx);
			const idx_t original_sel_idx = sel.get_index(flat_sel_idx);
			sel.set_index(idx, original_sel_idx);
		}
	} else {
		sel.Initialize(state.bf_sel);
	}

	approved_tuple_count = found_count;
	return approved_tuple_count;
}

bool BFTableFilter::FilterValue(const Value &value) const {
	const auto hash = value.Hash();
	return filter.LookupOne(hash);
}

template <class T>
static FilterPropagateResult TemplatedCheckStatistics(const BloomFilter &bf, const BaseStatistics &stats) {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::GetMin<T>(stats);
	const auto max = NumericStats::GetMax<T>(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Invalid stats
	}
	T range_typed;
	if (!TrySubtractOperator::Operation(max, min, range_typed) || range_typed > 2048) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Overflow or too wide of a range
	}
	const auto range = NumericCast<idx_t>(range_typed);

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

FilterPropagateResult BFTableFilter::CheckStatistics(BaseStatistics &stats) const {
	switch (stats.GetType().InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedCheckStatistics<uint8_t>(filter, stats);
	case PhysicalType::UINT16:
		return TemplatedCheckStatistics<uint16_t>(filter, stats);
	case PhysicalType::UINT32:
		return TemplatedCheckStatistics<uint32_t>(filter, stats);
	case PhysicalType::UINT64:
		return TemplatedCheckStatistics<uint64_t>(filter, stats);
	case PhysicalType::UINT128:
		return TemplatedCheckStatistics<uhugeint_t>(filter, stats);
	case PhysicalType::INT8:
		return TemplatedCheckStatistics<int8_t>(filter, stats);
	case PhysicalType::INT16:
		return TemplatedCheckStatistics<int16_t>(filter, stats);
	case PhysicalType::INT32:
		return TemplatedCheckStatistics<int32_t>(filter, stats);
	case PhysicalType::INT64:
		return TemplatedCheckStatistics<int64_t>(filter, stats);
	case PhysicalType::INT128:
		return TemplatedCheckStatistics<hugeint_t>(filter, stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

bool BFTableFilter::Equals(const TableFilter &other) const {
	if (!TableFilter::Equals(other)) {
		return false;
	}
	return false;
}
unique_ptr<TableFilter> BFTableFilter::Copy() const {
	return make_uniq<BFTableFilter>(this->filter, this->filters_null_values, this->key_column_name, this->key_type);
}

unique_ptr<Expression> BFTableFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant);
}

void BFTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
}

unique_ptr<TableFilter> BFTableFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	BloomFilter filter;
	auto result = make_uniq<BFTableFilter>(filter, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
