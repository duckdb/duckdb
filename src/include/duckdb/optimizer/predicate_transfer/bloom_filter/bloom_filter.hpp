//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstdint>
#include <mutex>

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

#ifndef BF_ASSUME_ALIGNED
#if defined(__GNUC__) || defined(__clang__)
#define BF_ASSUME_ALIGNED(x, align) __builtin_assume_aligned(x, align)
#else
// Fallback: just return the pointer as-is
#define BF_ASSUME_ALIGNED(x, align) (x)
#endif
#endif

namespace duckdb {

static constexpr const uint32_t MAX_NUM_BLOCKS = (1ULL << 22);
static constexpr const uint32_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr const uint32_t MIN_NUM_BITS = 512;
static constexpr const uint32_t LOG_BLOCK_SIZE = 5;

static constexpr const int32_t SIMD_BATCH_SIZE = 32;
static constexpr const int32_t SIMD_ALIGNMENT = 64;

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows, const vector<idx_t> &applied,
	                const vector<idx_t> &built);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;
	vector<idx_t> bound_cols_applied;
	vector<idx_t> bound_cols_built;

public:
	int Lookup(DataChunk &chunk, vector<uint32_t> &results);
	void Insert(DataChunk &chunk);

	uint32_t num_blocks;
	uint32_t num_blocks_log;

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

	inline uint32_t GetBlock1(uint32_t key_lo, uint32_t key_hi) const {
		// block: 13 bits in key_lo and 9 bits in key_hi
		// sector 1: 4 bits in key_lo
		return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_blocks - 1);
	}
	inline uint32_t GetBlock2(uint32_t key_hi, uint32_t block1) const {
		// sector 2: 3 bits in key_hi
		return block1 ^ (8 + (key_hi & 7));
	}

	inline void InsertOne(uint32_t key_lo, uint32_t key_hi, uint32_t *BF_RESTRICT bf) const {
		uint32_t block1 = GetBlock1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t block2 = GetBlock2(key_hi, block1);
		uint32_t mask2 = GetMask2(key_hi);
		bf[block1] |= mask1;
		bf[block2] |= mask2;
	}
	inline bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *BF_RESTRICT bf) const {
		uint32_t block1 = GetBlock1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t block2 = GetBlock2(key_hi, block1);
		uint32_t mask2 = GetMask2(key_hi);
		return ((bf[block1] & mask1) == mask1) & ((bf[block2] & mask2) == mask2);
	}

private:
	int BloomFilterLookup(int num, const uint64_t *BF_RESTRICT key64, const uint32_t *BF_RESTRICT bf,
	                      uint32_t *BF_RESTRICT out) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);

		// align the address of key
		int unaligned_num = (SIMD_ALIGNMENT - reinterpret_cast<size_t>(key) % SIMD_ALIGNMENT) / sizeof(uint64_t);
		unaligned_num = std::min<int>(unaligned_num, num);
		for (int i = 0; i < unaligned_num; i++) {
			out[i] = LookupOne(key[i * 2], key[i * 2 + 1], bf);
		}

		// auto vectorization
		int aligned_end = (num - unaligned_num) / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE + unaligned_num;
		uint32_t *BF_RESTRICT aligned_key =
		    reinterpret_cast<uint32_t * BF_RESTRICT>(BF_ASSUME_ALIGNED(&key[unaligned_num * 2], SIMD_ALIGNMENT));

		for (int i = 0; i < aligned_end - unaligned_num; i += SIMD_BATCH_SIZE) {
			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = aligned_key[p + p];
				uint32_t key_hi = aligned_key[p + p + 1];

				uint32_t block1 = GetBlock1(key_lo, key_hi);
				uint32_t mask1 = GetMask1(key_lo);
				uint32_t block2 = GetBlock2(key_hi, block1);
				uint32_t mask2 = GetMask2(key_hi);

				out[i + unaligned_num + j] = ((bf[block1] & mask1) == mask1) & ((bf[block2] & mask2) == mask2);
			}
		}

		// unaligned tail
		for (int i = aligned_end; i < num; i++) {
			out[i] = LookupOne(key[i * 2], key[i * 2 + 1], bf);
		}
		return num;
	}

	void BloomFilterInsert(int num, const uint64_t *BF_RESTRICT key64, uint32_t *BF_RESTRICT bf) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);

		// align the address of key
		int unaligned_num = (SIMD_ALIGNMENT - reinterpret_cast<size_t>(key) % SIMD_ALIGNMENT) / sizeof(uint64_t);
		unaligned_num = std::min<int>(unaligned_num, num);
		for (int i = 0; i < unaligned_num; i++) {
			InsertOne(key[i + i], key[i + i + 1], bf);
		}

		// auto vectorization
		int aligned_end = (num - unaligned_num) / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE + unaligned_num;
		uint32_t *BF_RESTRICT aligned_key =
		    reinterpret_cast<uint32_t * BF_RESTRICT>(BF_ASSUME_ALIGNED(&key[unaligned_num * 2], SIMD_ALIGNMENT));

		for (int i = 0; i < aligned_end - unaligned_num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = aligned_key[p + p];
				uint32_t key_hi = aligned_key[p + p + 1];

				block1[j] = GetBlock1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetBlock2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				bf[block1[j]] |= mask1[j];
				bf[block2[j]] |= mask2[j];
			}
		}

		// unaligned tail
		for (int i = aligned_end; i < num; i++) {
			InsertOne(key[i * 2], key[i * 2 + 1], bf);
		}
	}

	AllocatedData buf_;
};

} // namespace duckdb
