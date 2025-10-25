#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6; // a sector is 64 bits, log2(64) = 6
static constexpr idx_t SIMD_BATCH_SIZE = 128;
static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
static constexpr idx_t N_BITS = 4;                      // the number of bits to set per hash

// number of vectors to check before deciding on selectivity
static constexpr idx_t SELECTIVITY_N_VECTORS_TO_CHECK = 20;
// if the selectivity is higher than this, we disable the BF
static constexpr double SELECTIVITY_THRESHOLD = 0.25;

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {

	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	bitmask = num_sectors - 1;

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	bf = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(bf, num_sectors, 0);

	status.store(BloomFilterStatus::Active);
}

inline uint64_t GetMask(const uint8_t *__restrict shifts_8, const idx_t i) {
	uint64_t mask = 0;
	const uint64_t shift_8 = 8 * i;

	for (idx_t j = 8 - N_BITS; j < 8; j++) {
		const uint8_t bit_pos = shifts_8[shift_8 + j];
		mask |= (1ULL << bit_pos);
	}

	return mask;
}

static void InsertBlock(const uint64_t *__restrict keys, uint64_t *__restrict bf, const uint64_t bitmask) {

	uint64_t shifts[SIMD_BATCH_SIZE];
	const uint8_t *shifts_8 = reinterpret_cast<uint8_t *>(shifts);
	for (idx_t i = 0; i < SIMD_BATCH_SIZE; i++) {
		shifts[i] = keys[i] & SHIFT_MASK;
	}

	for (idx_t i = 0; i < SIMD_BATCH_SIZE; i++) {
		const uint64_t bf_offset = keys[i] & bitmask;
		const uint64_t mask = GetMask(shifts_8, i);
		std::atomic<uint64_t> &slot = *reinterpret_cast<std::atomic<uint64_t> *>(&bf[bf_offset]);
		slot.fetch_or(mask, std::memory_order_relaxed);
	}
}

void BloomFilter::InsertHashes(const Vector &hashes_v, idx_t count) const {

	auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	while (count >= SIMD_BATCH_SIZE) {
		InsertBlock(hashes, bf, bitmask);
		hashes += SIMD_BATCH_SIZE;
		count -= SIMD_BATCH_SIZE;
	}
	for (idx_t i = 0; i < count; i++) {
		InsertOne(hashes[i]);
	}
}

static void LookupBlock(const uint64_t *__restrict keys, const uint64_t *__restrict bf, uint64_t *__restrict found,
                        const uint64_t bitmask) {

	uint64_t shifts[SIMD_BATCH_SIZE];
	for (idx_t i = 0; i < SIMD_BATCH_SIZE; i++) {
		shifts[i] = keys[i] & SHIFT_MASK;
	}

	const auto shifts_8 = reinterpret_cast<uint8_t *>(shifts);
	for (idx_t i = 0; i < SIMD_BATCH_SIZE; i++) {
		const uint64_t bf_offset = keys[i] & bitmask;
		const uint64_t mask = GetMask(shifts_8, i);
		found[i] = (bf[bf_offset] & mask) == mask;
	}
}

idx_t BloomFilter::LookupHashes(const Vector &hashes_v, Vector &found_v, SelectionVector &result_sel,
                                const idx_t count) const {

	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);

	auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	const auto founds = FlatVector::GetData<uint64_t>(found_v);

	idx_t remaining_count = count;
	uint64_t *remaining_founds = founds;
	while (remaining_count >= SIMD_BATCH_SIZE) {
		LookupBlock(hashes, bf, remaining_founds, bitmask);
		hashes += SIMD_BATCH_SIZE;
		remaining_founds += SIMD_BATCH_SIZE;
		remaining_count -= SIMD_BATCH_SIZE;
	}
	for (idx_t i = 0; i < remaining_count; i++) {
		remaining_founds[i] = LookupOne(hashes[i]);
	}

	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		result_sel.set_index(found_count, i);
		found_count += founds[i];
	}
	return found_count;
}

inline void BloomFilter::InsertOne(const hash_t hash) const {

	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(shifts_8, 0);
	std::atomic<uint64_t> &slot = *reinterpret_cast<std::atomic<uint64_t> *>(&bf[bf_offset]);

	slot.fetch_or(mask, std::memory_order_relaxed);
}

inline bool BloomFilter::LookupOne(uint64_t hash) const {

	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(shifts_8, 0);

	return (bf[bf_offset] & mask) == mask;
}

string BFTableFilter::ToString(const string &column_name) const {
	if (filter.GetStatus().load() == BloomFilterStatus::Active) {
		return column_name + " IN BF(" + key_column_name + ")";
	} else {
		return "True";
	}
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
	if (!this->filter.IsActive()) {
		return approved_tuple_count;
	}

	if (state.current_capacity < approved_tuple_count) {
		state.hashes_v.Initialize(false, approved_tuple_count);
		state.found_v.Initialize(false, approved_tuple_count);
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
		found_count = this->filter.LookupHashes(state.hashes_v, state.found_v, state.bf_sel, approved_tuple_count);
	}

	// add the runtime statistics to stop using the bf if not selective
	auto &stats = this->filter.GetSelectivityStats();
	if (stats.vectors_processed.load() < SELECTIVITY_N_VECTORS_TO_CHECK) {
		stats.Update(found_count, approved_tuple_count);
		if (stats.vectors_processed.load() >= SELECTIVITY_N_VECTORS_TO_CHECK) {
			if (stats.GetSelectivity() >= SELECTIVITY_THRESHOLD) {
				this->filter.Pause();
			}
		}
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

FilterPropagateResult BFTableFilter::CheckStatistics(BaseStatistics &stats) const {
	if (this->filter.IsActive()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return FilterPropagateResult::FILTER_ALWAYS_TRUE;
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
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

void BFTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
	// todo: How/Should be serialize the bloom filter?
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
