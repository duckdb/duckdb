//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <random>

namespace duckdb {

template <typename T>
T SafeLoadAs(const uint8_t *unaligned) {
	T ret;
	std::memcpy(&ret, unaligned, sizeof(T));
	return ret;
}

// A set of pre-generated bit masks from a 64-bit word.
struct BloomFilterMasks {
	BloomFilterMasks();

	uint64_t mask(int bit_offset) {
		return (SafeLoadAs<uint64_t>(masks_ + bit_offset / 8) >> (bit_offset % 8)) & kFullMask;
	}

	static constexpr uint64_t kBitsPerMask = 57;
	static constexpr uint64_t kFullMask = (1ULL << kBitsPerMask) - 1;

	static constexpr uint64_t kMinBitsSet = 4;
	static constexpr uint64_t kMaxBitsSet = 5;

	static constexpr uint64_t kLogNumMasks = 10;
	static constexpr uint64_t kNumMasks = 1 << kLogNumMasks;

	static constexpr uint64_t kTotalBytes = (kNumMasks + 64) / 8;
	uint8_t masks_[kTotalBytes];

private:
	inline bool GetBit(const uint8_t *data, int bit_pos) const {
		return (data[bit_pos / 8] >> (bit_pos % 8)) & 1;
	}

	inline void SetBit(uint8_t *data, int bit_pos) {
		data[bit_pos / 8] |= (1 << (bit_pos % 8));
	}
};

class BlockedBloomFilter {
public:
	static constexpr int64_t MIN_NUM_BITS_PER_KEY = 8;
	static constexpr int64_t MIN_NUM_BITS = 512;
	static constexpr int64_t LOG_BLOCK_SIZE = 6;

public:
	BlockedBloomFilter() = default;

	void Initialize(ClientContext &context_p, size_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;
	uint64_t num_blocks_;

public:
	bool Find(uint64_t hash) const {
		uint64_t m = mask(hash);
		uint64_t b = blocks_[block_id(hash)];
		return (b & m) == m;
	}

	void Insert(uint64_t hash) {
		uint64_t m = mask(hash);
		std::atomic<uint64_t> &b = blocks_[block_id(hash)];
		b.fetch_or(m);
	}

	void InsertHashedData(DataChunk &chunk, const vector<idx_t> &cols);

public:
	// The columns applied this BF, and the columns used to build this BF
	vector<ColumnBinding> column_bindings_applied_;
	vector<ColumnBinding> column_bindings_built_;
	vector<idx_t> BoundColsApplied;
	vector<idx_t> BoundColsBuilt;

private:
	inline uint64_t mask(uint64_t hash) const {
		// The lowest bits of hash are used to pick mask index.
		int mask_id = static_cast<int>(hash & (BloomFilterMasks::kNumMasks - 1));
		uint64_t result = masks_.mask(mask_id);

		// The next set of hash bits is used to pick the amount of bit rotation of the mask.
		uint64_t rotation = (hash >> BloomFilterMasks::kLogNumMasks) & 63;
		return (result << rotation) | (result >> (64 - rotation));
	}

	inline uint64_t block_id(uint64_t hash) const {
		// The next set of hash bits following the bits used to select a mask is used to pick block id (index of 64-bit
		// word in a bit vector).
		return (hash >> (BloomFilterMasks::kLogNumMasks + LOG_BLOCK_SIZE)) & (num_blocks_ - 1);
	}

	static BloomFilterMasks masks_;

	AllocatedData buf_;
	std::atomic<uint64_t> *blocks_;
};
} // namespace duckdb
