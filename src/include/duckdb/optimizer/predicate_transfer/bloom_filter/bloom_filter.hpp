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
static constexpr const uint32_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr const uint32_t MIN_NUM_BITS = 512;
static constexpr const uint32_t LOG_BLOCK_SIZE = 5;

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;

public:
	size_t Lookup(DataChunk &chunk, vector<uint32_t> &results);
	void Insert(DataChunk &chunk);

	uint32_t num_blocks_;
	uint32_t num_blocks_log;

	std::mutex insert_lock;
	uint32_t *blocks_;

public:
	// The columns that this BF applies, and the columns used to build this BF
	vector<unique_ptr<Expression>> column_bindings_applied_;
	vector<unique_ptr<Expression>> column_bindings_built_;
	vector<idx_t> BoundColsApplied;
	vector<idx_t> BoundColsBuilt;

private:
	int BloomFilterLookup(int num, uint64_t *BF_RESTRICT key, uint32_t *BF_RESTRICT bf,
	                      uint32_t *BF_RESTRICT out) const {
		for (int i = 0; i < num; i++) {
			uint32_t block = (key[i] >> 20) & (num_blocks_ - 1);
			uint32_t mask = (1 << (key[i] & 31)) | (1 << ((key[i] >> 5) & 31)) | (1 << ((key[i] >> 10) & 31)) |
			                (1 << ((key[i] >> 15) & 31));
			out[i] = (bf[block] & mask) == mask;
		}
		return num;
	}

	void BloomFilterInsert(int num, uint64_t *BF_RESTRICT key, uint32_t *BF_RESTRICT bf) const {
		for (int i = 0; i < num; i++) {
			uint32_t block = (key[i] >> 20) & (num_blocks_ - 1);
			uint32_t mask = (1 << (key[i] & 31)) | (1 << ((key[i] >> 5) & 31)) | (1 << ((key[i] >> 10) & 31)) |
			                (1 << ((key[i] >> 15) & 31));
			bf[block] |= mask;
		}
	}

	AllocatedData buf_;
};

} // namespace duckdb
