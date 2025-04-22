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
#include <memory>
#include <mutex>
#include <random>
#include <cstring>

#ifndef BF_RESTRICT
#if defined(_MSC_VER)
#define BF_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#define BF_RESTRICT __restrict__
#else
#define BF_RESTRICT
#endif
#endif

namespace duckdb {

// A BF is at most (1 << 21) * 8 = 8 MB
static constexpr const uint32_t MAX_NUM_BLOCKS = (1ULL << 31);
static constexpr const uint32_t MIN_NUM_BITS_PER_KEY = 32;
static constexpr const uint32_t MIN_NUM_BITS = 512;
static constexpr const uint32_t LOG_BLOCK_SIZE = 5;

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
	int BloomFilterLookup(int num, const uint64_t *BF_RESTRICT key, const uint32_t *BF_RESTRICT bf,
	                      uint32_t *BF_RESTRICT out) const {
		for (int i = 0; i < num; i++) {
			uint32_t key_high = static_cast<uint32_t>(key[i] >> 32);
			uint32_t key_low = static_cast<uint32_t>(key[i]);

			uint32_t block = key_high & (num_blocks - 1);
			uint32_t mask = (1 << (key_low & 31)) | (1 << ((key_low >> 5) & 31)) | (1 << ((key_low >> 10) & 31)) |
			                (1 << ((key_low >> 15) & 31)) | (1 << ((key_low >> 20) & 31)) |
			                (1 << ((key_low >> 25) & 31));
			out[i] = (bf[block] & mask) == mask;
		}
		return num;
	}

	void BloomFilterInsert(int num, const uint64_t *BF_RESTRICT key, uint32_t *BF_RESTRICT bf) const {
		for (int i = 0; i < num; i++) {
			uint32_t key_high = static_cast<uint32_t>(key[i] >> 32);
			uint32_t key_low = static_cast<uint32_t>(key[i]);

			uint32_t block = key_high & (num_blocks - 1);
			uint32_t mask = (1 << (key_low & 31)) | (1 << ((key_low >> 5) & 31)) | (1 << ((key_low >> 10) & 31)) |
			                (1 << ((key_low >> 15) & 31)) | (1 << ((key_low >> 20) & 31)) |
			                (1 << ((key_low >> 25) & 31));
			bf[block] |= mask;
		}
	}

	AllocatedData buf_;
};

} // namespace duckdb
