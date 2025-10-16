//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/bloom_filter
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

static constexpr const idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr const idx_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr const idx_t MIN_NUM_BITS = 512;
static constexpr const idx_t LOG_SECTOR_SIZE = 5;
static constexpr const idx_t SIMD_BATCH_SIZE = 16;

class CacheSectorizedBloomFilter {

public:
	CacheSectorizedBloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t est_num_rows) {

		context = &context_p;
		buffer_manager = &BufferManager::GetBufferManager(*context);

		const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
		num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
		num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

		buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
		// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
		blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
		std::fill_n(blocks, num_sectors, 0);

		initialized = true;
	}

	ClientContext *context;
	BufferManager *buffer_manager;

	bool initialized = false;
	bool finalized_;

public:
	idx_t LookupHashes(Vector &hashes, SelectionVector &result_sel, const idx_t count_p) const;
	bool LookupHash(hash_t hash) const;
	void InsertHashes(const Vector &hashes, idx_t count, bool parallel);

	bool IsInitialized() const {
		return initialized;
	}

	idx_t num_sectors;
	idx_t num_sectors_log;

	std::mutex insert_lock;
	uint32_t *blocks;

private:
	// key_lo |5:bit3|5:bit2|5:bit1|  13:block    |4:sector1 | bit layout (32:total)
	// key_hi |5:bit4|5:bit3|5:bit2|5:bit1|9:block|3:sector2 | bit layout (32:total)
	inline uint32_t GetMask1(uint32_t key_lo) const {
		// 3 bits in key_lo
		return (1u << ((key_lo >> 17) & 31)) | (1u << ((key_lo >> 22) & 31)) | (1u << ((key_lo >> 27) & 31));
	}
	inline uint32_t GetMask2(uint32_t key_hi) const {
		// 4 bits in key_hi
		return (1u << ((key_hi >> 12) & 31)) | (1u << ((key_hi >> 17) & 31)) | (1u << ((key_hi >> 22) & 31)) |
		       (1u << ((key_hi >> 27) & 31));
	}

	inline uint32_t GetSector1(uint32_t key_lo, uint32_t key_hi) const {
		// block: 13 bits in key_lo and 9 bits in key_hi
		// sector 1: 4 bits in key_lo
		return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_sectors - 1);
	}
	inline uint32_t GetSector2(uint32_t key_hi, uint32_t block1) const {
		// sector 2: 3 bits in key_hi
		return block1 ^ (8 + (key_hi & 7));
	}

	inline void InsertOne(const uint32_t key_lo, const uint32_t key_hi, uint32_t *__restrict bf) const {
		const uint32_t sector1 = GetSector1(key_lo, key_hi);
		const uint32_t mask1 = GetMask1(key_lo);
		const uint32_t sector2 = GetSector2(key_hi, sector1);
		const uint32_t mask2 = GetMask2(key_hi);
		bf[sector1] |= mask1;
		bf[sector2] |= mask2;
		// todo: There is duplicate code here we should get one function to get the two masks
	}
	inline bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *__restrict bf) const {
		const uint32_t sector1 = GetSector1(key_lo, key_hi);
		const uint32_t mask1 = GetMask1(key_lo);
		const uint32_t sector2 = GetSector2(key_hi, sector1);
		const uint32_t mask2 = GetMask2(key_hi);
		return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
	}

private:
	idx_t BloomFilterLookup(const hash_t *__restrict hashes, const uint32_t *__restrict bf, SelectionVector &found_sel,
	                        const idx_t num) const {
		const auto key = reinterpret_cast<const uint32_t *__restrict>(hashes);
		idx_t found_count = 0;
		for (idx_t i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				idx_t p = i + j;
				const uint32_t key_lo = key[p + p];
				const uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				const bool hit = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
				found_sel.set_index(found_count, i + j);
				found_count += hit;
			}
		}

		// unaligned tail
		for (idx_t i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			const bool hit = LookupOne(key[i + i], key[i + i + 1], bf);
			found_sel.set_index(found_count, i);
			found_count += hit;
		}
		return found_count;
	}

	void BloomFilterInsert(const idx_t num, const uint64_t *__restrict key64, uint32_t *__restrict bf) const {
		const uint32_t *__restrict key = reinterpret_cast<const uint32_t *__restrict>(key64);
		for (idx_t i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				idx_t p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				bf[block1[j]] |= mask1[j];
				bf[block2[j]] |= mask2[j];
			}
		}

		// unaligned tail
		for (idx_t i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			InsertOne(key[i + i], key[i + i + 1], bf);
		}
	}

	AllocatedData buf_;
};

class BloomFilter : public TableFilter {

private:
	CacheSectorizedBloomFilter &filter;

	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::BLOOM_FILTER;

public:
	explicit BloomFilter(CacheSectorizedBloomFilter &filter_p, const bool filters_null_values_p,
	                     const string &key_column_name_p, const LogicalType &key_type_p)
	    : TableFilter(TYPE), filter(filter_p), filters_null_values(filters_null_values_p),
	      key_column_name(key_column_name_p), key_type(key_type_p) {
	}

	//! If the join condition is e.g. "A = B", the bf will filter null values.
	//! If the condition is "A is B" the filter will let nulls pass
	bool FiltersNullValues() const {
		return filters_null_values;
	}

	LogicalType GetKeyType() const {
		return key_type;
	}

public:
	string ToString(const string &column_name) const override;

	// todo: we can remove this
	void HashInternal(Vector &keys_v, const SelectionVector &sel, idx_t &approved_count,
	                                            BloomFilterState &state) const {
		if (sel.IsSet()) {
			state.keys_sliced_v.Slice(keys_v, sel, approved_count);
			VectorOperations::Hash(state.keys_sliced_v, state.hashes_v,
			                       approved_count); // todo: we actually only want to hash the sel!

		} else {
			VectorOperations::Hash(keys_v, state.hashes_v,
			                       approved_count); // todo: we actually only want to hash the sel!
		}
	}

	// Filters the data by first hashing and then probing the bloom filter. The &sel will hold
	// the remaining tuples, &approved_tuple_count will hold the approved count.
	__attribute__((noinline)) idx_t Filter(Vector &keys_v, UnifiedVectorFormat &keys_uvf, SelectionVector &sel, idx_t &approved_tuple_count,
	             BloomFilterState &state) const {

		// printf("Filter bf: bf has %llu sectors and initialized=%hd \n", filter.num_sectors, filter.IsInitialized());
		if (!this->filter.IsInitialized() || !state.continue_filtering) {
			return approved_tuple_count; // todo: may
		}

		if (state.current_capacity < approved_tuple_count) {
			state.hashes_v.Initialize(false, approved_tuple_count);
			state.bf_sel.Initialize(approved_tuple_count);
			state.current_capacity = approved_tuple_count;
		}

		HashInternal(keys_v, sel, approved_tuple_count, state);

		// todo: we need to properly find out how one would densify the hashes here!
		idx_t found_count;
		if (state.hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			const auto &hash = *ConstantVector::GetData<hash_t>(state.hashes_v);
			const bool found = this->filter.LookupHash(hash);
			found_count = found ? approved_tuple_count : 0;
		} else {
			state.hashes_v.Flatten(approved_tuple_count);
			found_count = this->filter.LookupHashes(state.hashes_v, state.bf_sel, approved_tuple_count);
		}

		// add the runtime statistics to stop using the bf if not selective
		if (state.vectors_processed < 40) {
			state.vectors_processed += 1;
			state.tuples_accepted += found_count;
			state.tuples_processed += approved_tuple_count;

			if (state.vectors_processed == 40) {
				const double selectivity =
				    static_cast<double>(state.tuples_accepted) / static_cast<double>(state.tuples_processed);
				// printf("%f\n", selectivity);
				if (selectivity > 0.8) {
					state.continue_filtering = false;
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
			sel.Initialize(state.bf_sel); // maybe we could also reference here?
		}

		approved_tuple_count = found_count;
		return approved_tuple_count;
	}

	bool FilterValue(const Value &value) const {
		const auto hash = value.Hash();
		return filter.LookupHash(hash);
	}

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	bool Equals(const TableFilter &other) const override {
		if (!TableFilter::Equals(other)) {
			return false;
		}

		// todo: not really sure what to do here :(
		return false;
	}
	unique_ptr<TableFilter> Copy() const override {
		return make_uniq<BloomFilter>(this->filter, this->filters_null_values, this->key_column_name, this->key_type);
	}

	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override {
		TableFilter::Serialize(serializer);
		serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
		serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
		serializer.WriteProperty<LogicalType>(202, "key_type", key_type);

		// todo: How/Should be serialize the bloom filter?
	}
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		bool filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
		string key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
		LogicalType key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

		CacheSectorizedBloomFilter filter;
		auto result = make_uniq<BloomFilter>(filter, filters_null_values, key_column_name, key_type);
		return std::move(result);
	}
};

} // namespace duckdb
