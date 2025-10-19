#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 5;
static constexpr idx_t SIMD_BATCH_SIZE = 16;

// number of vectors to check before deciding on selectivity
static constexpr id_t SELECTIVITY_N_VECTORS_TO_CHECK = 20;
// if the selectivity is higher than this, we disable the BF
static constexpr double SELECTIVITY_THRESHOLD = 0.33;

void CacheSectorizedBloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {

	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, num_sectors, 0);

	state.store(State::Active);
}

void CacheSectorizedBloomFilter::InsertHashes(const Vector &hashes, const idx_t count) const {

	const auto hashes_64 = reinterpret_cast<const uint64_t *>(hashes.GetData());
	const auto key_32 = reinterpret_cast<const uint32_t *__restrict>(hashes_64);

	for (idx_t i = 0; i + SIMD_BATCH_SIZE <= count; i += SIMD_BATCH_SIZE) {
		uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
		uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			idx_t p = i + j;
			const uint32_t key_lo = key_32[p + p];
			const uint32_t key_hi = key_32[p + p + 1];
			block1[j] = GetSector1(key_lo, key_hi);
			mask1[j] = GetMask1(key_lo);
			block2[j] = GetSector2(key_hi, block1[j]);
			mask2[j] = GetMask2(key_hi);
		}

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			// Atomic OR operation
			std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&blocks[block1[j]]);
			std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&blocks[block2[j]]);

			atomic_bf1.fetch_or(mask1[j], std::memory_order_relaxed);
			atomic_bf2.fetch_or(mask2[j], std::memory_order_relaxed);
		}
	}

	// unaligned tail
	for (idx_t i = count & ~(SIMD_BATCH_SIZE - 1); i < count; i++) {
		InsertOne(key_32[i + i], key_32[i + i + 1], blocks);
	}
}

idx_t CacheSectorizedBloomFilter::LookupHashes(const Vector &hashes, SelectionVector &result_sel,
                                               const idx_t count) const {

	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes.GetType() == LogicalType::HASH);

	const auto hashes_64 = reinterpret_cast<const uint64_t *>(hashes.GetData());
	const auto key_32 = reinterpret_cast<const uint32_t *__restrict>(hashes_64);

	idx_t found_count = 0;
	for (idx_t i = 0; i + SIMD_BATCH_SIZE <= count; i += SIMD_BATCH_SIZE) {
		uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
		uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			idx_t p = i + j;
			const uint32_t key_lo = key_32[p + p];
			const uint32_t key_hi = key_32[p + p + 1];
			block1[j] = GetSector1(key_lo, key_hi);
			mask1[j] = GetMask1(key_lo);
			block2[j] = GetSector2(key_hi, block1[j]);
			mask2[j] = GetMask2(key_hi);
		}

		for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
			const bool hit =
			    ((blocks[block1[j]] & mask1[j]) == mask1[j]) & ((blocks[block2[j]] & mask2[j]) == mask2[j]);
			result_sel.set_index(found_count, i + j);
			found_count += hit;
		}
	}

	// unaligned tail
	for (idx_t i = count & ~(SIMD_BATCH_SIZE - 1); i < count; i++) {
		const bool hit = LookupOne(key_32[i + i], key_32[i + i + 1], blocks);
		result_sel.set_index(found_count, i);
		found_count += hit;
	}
	return found_count;
}

bool CacheSectorizedBloomFilter::LookupHash(hash_t hash) const {
	// Reinterpret the address of a value as a pointer to uint32_t
	const uint32_t *parts = reinterpret_cast<uint32_t *>(&hash);

	const uint32_t lower = parts[0];
	const uint32_t higher = parts[1];

	return LookupOne(lower, higher, blocks);
}

uint32_t CacheSectorizedBloomFilter::GetMask1(const uint32_t key_lo) {
	// 3 bits in key_lo
	return (1u << ((key_lo >> 17) & 31)) | (1u << ((key_lo >> 22) & 31)) | (1u << ((key_lo >> 27) & 31));
}

uint32_t CacheSectorizedBloomFilter::GetMask2(const uint32_t key_hi) {
	// 4 bits in key_hi
	return (1u << ((key_hi >> 12) & 31)) | (1u << ((key_hi >> 17) & 31)) | (1u << ((key_hi >> 22) & 31)) |
	       (1u << ((key_hi >> 27) & 31));
}

uint32_t CacheSectorizedBloomFilter::GetSector1(const uint32_t key_lo, const uint32_t key_hi) const {
	// block: 13 bits in key_lo and 9 bits in key_hi
	// sector 1: 4 bits in key_lo
	return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_sectors - 1);
}

uint32_t CacheSectorizedBloomFilter::GetSector2(const uint32_t key_hi, const uint32_t block1) const {
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

bool CacheSectorizedBloomFilter::LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *__restrict bf) const {
	const uint32_t sector1 = GetSector1(key_lo, key_hi);
	const uint32_t mask1 = GetMask1(key_lo);
	const uint32_t sector2 = GetSector2(key_hi, sector1);
	const uint32_t mask2 = GetMask2(key_hi);
	return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
}

string BloomFilter::ToString(const string &column_name) const {
	if (filter.GetState().load() == CacheSectorizedBloomFilter::State::Active) {
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
		found_count = this->filter.LookupHashes(state.hashes_v, state.bf_sel, approved_tuple_count);
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
