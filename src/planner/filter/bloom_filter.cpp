#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6;
static constexpr idx_t SIMD_BATCH_SIZE = 128;

// number of vectors to check before deciding on selectivity
static constexpr idx_t SELECTIVITY_N_VECTORS_TO_CHECK = 20;
// if the selectivity is higher than this, we disable the BF
static constexpr double SELECTIVITY_THRESHOLD = 0.33;

void CacheSectorizedBloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {

	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, num_sectors, 0);

	for (idx_t b = 0; b < 256; b++) {
		uint64_t positive = 0;
		for (idx_t i = 0; i < 8; i++) {
			positive |= (b >> i & 1) << (i * 8);
		}
		const uint64_t negative = (~positive) & 0x0101010101010101ULL;

		spread_table[2 * b] = positive;
		spread_table[2 * b + 1] = negative;
	}

	state.store(BloomFilterState::Active);
}

static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
static constexpr idx_t N_BITS = 4;

void CacheSectorizedBloomFilter::InsertHashes(const Vector &hashes_v, const idx_t count) const {

	const auto hashes = reinterpret_cast<const uint64_t *>(hashes_v.GetData());
	const uint64_t bitmask = num_sectors - 1;
	for (uint64_t i = 0; i + SIMD_BATCH_SIZE <= count; i += SIMD_BATCH_SIZE) {

		uint64_t shifts[SIMD_BATCH_SIZE];
		uint8_t *shifts_8 = reinterpret_cast<uint8_t *>(const_cast<uint64_t *>(shifts));

		const uint64_t *key64_batch = &hashes[i];

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			shifts[j] = key64_batch[j] & SHIFT_MASK;
		}

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			const uint64_t offset_pos = 8 * j;

			uint64_t seed = key64_batch[j];
			// seed *= 0xbf58476d1ce4e5b9ULL;
			// seed ^= seed >> 27;
			const uint64_t bf_offset = seed & bitmask;

			uint64_t mask = 0;
			for (idx_t k = 8 - N_BITS; k < 8; k++) {
				const uint8_t bit_pos = shifts_8[offset_pos + k];
				mask |= (1ULL << bit_pos);
			}
			std::atomic<uint64_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint64_t> *>(&blocks[bf_offset]);
			atomic_bf2.fetch_or(mask, std::memory_order_relaxed);
		}
	}
}

idx_t CacheSectorizedBloomFilter::LookupHashes(const Vector &hashes_v, Vector &found_v, SelectionVector &result_sel,
                                               const idx_t count) const {

	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);

	const auto hashes_64 = reinterpret_cast<const uint64_t *>(hashes_v.GetData());

	auto found_bools = FlatVector::GetData<uint32_t>(found_v);

	LookupHashesInternal(hashes_64, found_bools, blocks, count);

	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		result_sel.set_index(found_count, i);
		found_count += found_bools[i];
	}
	return found_count;
}

void CacheSectorizedBloomFilter::LookupHashesInternal(const uint64_t *__restrict hashes, uint32_t *__restrict founds,
                                                      const uint64_t *__restrict bf, const idx_t count) const {

	const uint64_t bitmask = num_sectors - 1;

	for (uint64_t i = 0; i + SIMD_BATCH_SIZE <= count; i += SIMD_BATCH_SIZE) {

		uint64_t shifts[SIMD_BATCH_SIZE];
		uint8_t *shifts_8 = reinterpret_cast<uint8_t *>(const_cast<uint64_t *>(shifts));

		const uint64_t *key64_batch = &hashes[i];
		uint32_t *found_batch = &founds[i];

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			shifts[j] = key64_batch[j] & SHIFT_MASK;
		}

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			const uint64_t offset_pos = 8 * j;

			uint64_t seed = key64_batch[j];
			// seed *= 0xbf58476d1ce4e5b9ULL;
			// seed ^= seed >> 27;
			const uint64_t bf_offset = seed & bitmask;

			uint64_t mask = 0;
			for (idx_t k = 8 - N_BITS; k < 8; k++) {
				const uint8_t bit_pos = shifts_8[offset_pos + k];
				mask |= (1ULL << bit_pos);
			}

			found_batch[j] = (bf[bf_offset] & mask) == mask;
		}
	}
	// Todo: Handle unaligned tail
	for (uint64_t i = count & ~(SIMD_BATCH_SIZE - 1); i < count; i++) {
		founds[i] = true;
	}
}

bool CacheSectorizedBloomFilter::LookupHash(hash_t hash_p) const {

	uint64_t masks[2];
	uint64_t shifts[2];

	uint64_t hash[2] = {hash_p, hash_p};
	const uint64_t offset = (hash[0] >> 14) & 0xFFFF;

	const uint64_t blocks_buffer[2] = {blocks[offset], blocks[offset + 1]};

	const uint64_t spread_table_offset = (hash_p & 0xFF) * 2;
	masks[0] = spread_table[spread_table_offset];
	masks[1] = spread_table[spread_table_offset + 1];

	// maxiumn shift amount is 7, so we need 3 bits
	shifts[0] = (hash[0] >> 8) & 0x7;
	shifts[1] = (hash[1] >> 11) & 0x7;

	masks[0] = masks[0] << shifts[0];
	masks[1] = masks[1] << shifts[1];

	return (masks[0] & blocks_buffer[0]) == masks[0] & (masks[1] & blocks_buffer[1]) == masks[1];
}

inline uint32_t CacheSectorizedBloomFilter::GetMask1(const uint32_t key_lo) {
	// 3 bits in key_lo
	return (1u << ((key_lo >> 17) & 31)) | (1u << ((key_lo >> 22) & 31)) | (1u << ((key_lo >> 27) & 31));
}

inline uint32_t CacheSectorizedBloomFilter::GetMask2(const uint32_t key_hi) {
	// 4 bits in key_hi
	return (1u << ((key_hi >> 12) & 31)) | (1u << ((key_hi >> 17) & 31)) | (1u << ((key_hi >> 22) & 31)) |
	       (1u << ((key_hi >> 27) & 31));
}

inline uint32_t CacheSectorizedBloomFilter::GetSector1(const uint32_t key_lo, const uint32_t key_hi) const {
	// block: 13 bits in key_lo and 9 bits in key_hi
	// sector 1: 4 bits in key_lo
	return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_sectors - 1);
}

inline uint32_t CacheSectorizedBloomFilter::GetSector2(const uint32_t key_hi, const uint32_t block1) const {
	// sector 2: 3 bits in key_hi
	return block1 ^ (8 + (key_hi & 7));
}

void CacheSectorizedBloomFilter::InsertOne(const uint32_t key_lo, const uint32_t key_hi,
                                           uint32_t *__restrict bf) const {
	const uint32_t sector1 = GetSector1(key_lo, key_hi);
	const uint32_t mask1 = GetMask1(key_lo);
	const uint32_t sector2 = GetSector2(key_hi, sector1);
	const uint32_t mask2 = GetMask2(key_hi);

	// Perform atomic OR operation on the bf array elements using std::atomic
	std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector1]);
	std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector2]);

	atomic_bf1.fetch_or(mask1, std::memory_order_relaxed);
	atomic_bf2.fetch_or(mask2, std::memory_order_relaxed);
}

inline bool CacheSectorizedBloomFilter::LookupOne(uint32_t key_lo, uint32_t key_hi,
                                                  const uint32_t *__restrict bf) const {
	const uint32_t sector1 = GetSector1(key_lo, key_hi);
	const uint32_t mask1 = GetMask1(key_lo);
	const uint32_t sector2 = GetSector2(key_hi, sector1);
	const uint32_t mask2 = GetMask2(key_hi);
	return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
}

string BloomFilter::ToString(const string &column_name) const {
	if (filter.GetState().load() == CacheSectorizedBloomFilter::BloomFilterState::Active) {
		return column_name + " IN BF(" + key_column_name + ")";
	} else {
		return "True";
	}
}

void BloomFilter::HashInternal(Vector &keys_v, const SelectionVector &sel, const idx_t approved_count,
                               BloomFilterState &state) const {
	if (sel.IsSet()) {
		state.keys_sliced_v.Slice(keys_v, sel, approved_count);
		VectorOperations::Hash(state.keys_sliced_v, state.hashes_v, approved_count);
	} else {
		VectorOperations::Hash(keys_v, state.hashes_v, approved_count);
	}
}

idx_t BloomFilter::Filter(Vector &keys_v, SelectionVector &sel, idx_t &approved_tuple_count,
                          BloomFilterState &state) const {
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
		const bool found = this->filter.LookupHash(constant_hash);
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

bool BloomFilter::FilterValue(const Value &value) const {
	const auto hash = value.Hash();
	return filter.LookupHash(hash);
}

FilterPropagateResult BloomFilter::CheckStatistics(BaseStatistics &stats) const {
	if (this->filter.IsActive()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return FilterPropagateResult::FILTER_ALWAYS_TRUE;
}

bool BloomFilter::Equals(const TableFilter &other) const {
	if (!TableFilter::Equals(other)) {
		return false;
	}
	return false;
}
unique_ptr<TableFilter> BloomFilter::Copy() const {
	return make_uniq<BloomFilter>(this->filter, this->filters_null_values, this->key_column_name, this->key_type);
}

unique_ptr<Expression> BloomFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

void BloomFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
	// todo: How/Should be serialize the bloom filter?
}

unique_ptr<TableFilter> BloomFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	CacheSectorizedBloomFilter filter;
	auto result = make_uniq<BloomFilter>(filter, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
