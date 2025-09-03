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

#ifndef BF_RESTRICT
#if defined(_MSC_VER)
#define BF_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#define BF_RESTRICT __restrict__
#else
// Fallback: just return the pointer as-is
#define BF_RESTRICT
#endif
#endif

class CacheSectorizedBloomFilter {

	static constexpr const idx_t MAX_NUM_SECTORS = (1ULL << 26);
	static constexpr const idx_t MIN_NUM_BITS_PER_KEY = 16;
	static constexpr const idx_t MIN_NUM_BITS = 512;
	static constexpr const idx_t LOG_SECTOR_SIZE = 5;
	static constexpr const idx_t SIMD_BATCH_SIZE = 16;

public:
	CacheSectorizedBloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;

public:
	idx_t Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied) const;
	idx_t LookupHashes(Vector &hashes, const idx_t count_p, vector<uint32_t> &results) const;
	void InsertKeys(DataChunk &chunk, const vector<idx_t> &bound_cols_built);
	void InsertHashes(Vector hashes, const idx_t count);

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

	inline void InsertOne(const uint32_t key_lo, const uint32_t key_hi, uint32_t *BF_RESTRICT bf) const {
		const uint32_t sector1 = GetSector1(key_lo, key_hi);
		const uint32_t mask1 = GetMask1(key_lo);
		const uint32_t sector2 = GetSector2(key_hi, sector1);
		const uint32_t mask2 = GetMask2(key_hi);
		bf[sector1] |= mask1;
		bf[sector2] |= mask2;

		// todo: There is duplicate code here we should get one function to get the two masks
	}
	inline bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *BF_RESTRICT bf) const {
		const uint32_t sector1 = GetSector1(key_lo, key_hi);
		const uint32_t mask1 = GetMask1(key_lo);
		const uint32_t sector2 = GetSector2(key_hi, sector1);
		const uint32_t mask2 = GetMask2(key_hi);
		return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
	}

private:
	idx_t BloomFilterLookup(const idx_t num, const uint64_t *BF_RESTRICT key64, const uint32_t *BF_RESTRICT bf,
	                        uint32_t *BF_RESTRICT out) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
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
				out[i + j] = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
			}
		}

		// unaligned tail
		for (idx_t i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			out[i] = LookupOne(key[i + i], key[i + i + 1], bf);
		}
		return num;
	}

	void BloomFilterInsert(const idx_t num, const uint64_t *BF_RESTRICT key64, uint32_t *BF_RESTRICT bf) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
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
public:
	static constexpr auto TYPE = TableFilterType::BLOOM_FILTER;

public:
	explicit BloomFilter(unique_ptr<CacheSectorizedBloomFilter> filter_p) : TableFilter(TYPE), filter(std::move(filter_p)) {
	}

	unique_ptr<CacheSectorizedBloomFilter> filter;

public:
	// FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	// string ToString(const string &column_name) const override;
	// bool Equals(const TableFilter &other) const override;
	// unique_ptr<TableFilter> Copy() const override;
	// unique_ptr<Expression> ToExpression(const Expression &column) const override;
	// void Serialize(Serializer &serializer) const override;
	// static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
