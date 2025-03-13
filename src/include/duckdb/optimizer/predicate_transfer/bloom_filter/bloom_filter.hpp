//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <memory>

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
static size_t BloomFilterLookup(size_t num, size_t num_blocks_log, uint64_t *__restrict__ key,
                                uint64_t *__restrict__ bf, uint64_t *__restrict__ out) {
	for (int i = 0; i < num; i++) {
		uint64_t block = (key[i] >> (64 - num_blocks_log)) & ((1 << num_blocks_log) - 1);
		uint64_t mask = (1ULL << ((key[i] >> 0) & 63)) | (1ULL << ((key[i] >> 6) & 63)) |
		                (1ULL << ((key[i] >> 12) & 63)) | (1ULL << ((key[i] >> 18) & 63)) |
		                (1ULL << ((key[i] >> 24) & 63)) | (1ULL << ((key[i] >> 30) & 63)) |
		                (1ULL << ((key[i] >> 36) & 63));
		out[i] = (bf[block] & mask) == mask;
	}
	return num;
}

static void BloomFilterInsert(size_t num, size_t num_blocks_log, uint64_t *__restrict__ key,
                              uint64_t *__restrict__ bf) {
	for (int i = 0; i < num; i++) {
		uint64_t block = (key[i] >> (64 - num_blocks_log)) & ((1 << num_blocks_log) - 1);
		uint64_t mask = (1ULL << ((key[i] >> 0) & 63)) | (1ULL << ((key[i] >> 6) & 63)) |
		                (1ULL << ((key[i] >> 12) & 63)) | (1ULL << ((key[i] >> 18) & 63)) |
		                (1ULL << ((key[i] >> 24) & 63)) | (1ULL << ((key[i] >> 30) & 63)) |
		                (1ULL << ((key[i] >> 36) & 63));
		bf[block] |= mask;
	}
}

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, size_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;

public:
	size_t Lookup(DataChunk &chunk, vector<uint64_t> &results);
	void Insert(DataChunk &chunk);

	uint32_t num_blocks_;
	size_t num_blocks_log;

	std::mutex insert_lock;
	uint64_t *blocks_;

public:
	// The columns that this BF applies, and the columns used to build this BF
	vector<ColumnBinding> column_bindings_applied_;
	vector<ColumnBinding> column_bindings_built_;
	vector<idx_t> BoundColsApplied;
	vector<idx_t> BoundColsBuilt;

private:
	AllocatedData buf_;
};
} // namespace duckdb
