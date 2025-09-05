//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/bloom_filter
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
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

		printf("Initializing bf for %llu rows \n", est_num_rows);
		context = &context_p;
		buffer_manager = &BufferManager::GetBufferManager(*context);

		idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
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
	void InsertHashes(Vector hashes, const idx_t count);

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
			bool hit = LookupOne(key[i + i], key[i + i + 1], bf);
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

public:
	static constexpr auto TYPE = TableFilterType::BLOOM_FILTER;

public:
	explicit BloomFilter(CacheSectorizedBloomFilter &filter_p) : TableFilter(TYPE), filter(filter_p) {
	}

public:
	__attribute__((noinline)) // todo: we can remove this
	void
	HashInternal(Vector &keys_v, Vector &hashes_v, SelectionVector &sel, idx_t &approved_count) const {
		if (sel.IsSet()) {
			Vector keys_flat(keys_v.GetType(), approved_count);
			VectorOperations::Copy(keys_v, keys_flat, sel, approved_count, 0, 0);
			D_ASSERT(keys_flat.GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Hash(keys_flat, hashes_v, approved_count); // todo: we actually only want to hash the sel!

		} else {
			VectorOperations::Hash(keys_v, hashes_v, approved_count); // todo: we actually only want to hash the sel!
		}
	}

	// Filters the data by first hashing and then probing the bloom filter. The &sel will hold
	// the remaining tuples, &approved_tuple_count will hold the approved count.
	idx_t Filter(Vector keys_v, UnifiedVectorFormat &keys_uvf, SelectionVector &sel,
	             idx_t &approved_tuple_count) const {

		// printf("Filter bf: bf has %llu sectors and initialized=%hd \n", filter.num_sectors, filter.IsInitialized());
		if (!this->filter.IsInitialized()) {
			return approved_tuple_count;
		}

		Vector hashes_v(LogicalType::HASH, approved_tuple_count);

		HashInternal(keys_v, hashes_v, sel, approved_tuple_count);

		// todo: we need to properly find out how one would densify the hashes here!
		SelectionVector bf_sel(approved_tuple_count);
		const idx_t found_count = this->filter.LookupHashes(hashes_v, bf_sel, approved_tuple_count);

		if (sel.IsSet()) {
			for (idx_t idx = 0; idx < found_count; idx++) {
				const idx_t flat_sel_idx = bf_sel.get_index(idx);
				const idx_t original_sel_idx = sel.get_index(flat_sel_idx);
				sel.set_index(idx, original_sel_idx);
			}
		} else {
			sel.Initialize(bf_sel);
		}
		approved_tuple_count = found_count;
		return found_count;
	}
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	string ToString(const string &column_name) const override {
		return "BF Lookup";
	};
	bool Equals(const TableFilter &other) const override {
		if (!TableFilter::Equals(other)) {
			return false;
		}

		// todo: not really sure what to do here :(
		return false;
	}
	unique_ptr<TableFilter> Copy() const override {
		return make_uniq<BloomFilter>(this->filter);
	}

	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override {
		printf("IDK How to do this\n");
	}
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		printf("IDK How to do this\n");
	}
};

} // namespace duckdb
