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
static constexpr const uint32_t MAX_NUM_BLOCKS = (1 << 20);
static constexpr const uint32_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr const uint32_t MIN_NUM_BITS = 512;
static constexpr const uint32_t LOG_BLOCK_SIZE = 6;

// A set of pre-generated bit masks from a 64-bit word. https://save-buffer.github.io/bloom_filter.html
struct BloomFilterMasks {
	// Generate all masks as a single bit vector. Each bit offset in this bit
	// vector corresponds to a single mask.
	// In each consecutive kBitsPerMask bits, there must be between
	// kMinBitsSet and kMaxBitsSet bits set.
	BloomFilterMasks();

	uint64_t Mask(uint64_t hash) {
		// (Last kNumMasks Used) The lowest bits of hash are used to pick mask index.
		int mask_id = static_cast<int>(hash & (kNumMasks - 1));
		uint64_t result = GetMask(mask_id);

		// () The next set of hash bits is used to pick the amount of bit rotation of the mask.
		int rotation = static_cast<int>((hash >> kLogNumMasks) & 63);
		result = ROTL64(result, rotation);

		return result;
	}

	// Masks are 57 bits long because then they can be accessed at an
	// arbitrary bit offset using a single unaligned 64-bit load instruction.
	static constexpr int kBitsPerMask = 57;
	static constexpr uint64_t kFullMask = (1ULL << kBitsPerMask) - 1;

	// Minimum and maximum number of bits set in each mask.
	// This constraint is enforced when generating the bit masks.
	// Values should be close to each other and chosen as to minimize a Bloom
	// filter false positives rate.
	static constexpr int kMinBitsSet = 4;
	static constexpr int kMaxBitsSet = 5;

	// Number of generated masks.
	// Having more masks to choose will improve false positives rate of Bloom
	// filter but will also use more memory, which may lead to more CPU cache
	// misses.
	// The chosen value results in using only a few cache-lines for mask lookups,
	// while providing a good variety of available bit masks.
	static constexpr int kLogNumMasks = 10;
	static constexpr int kNumMasks = 1 << kLogNumMasks;

	// Data of masks. Masks are stored in a single bit vector. Nth mask is
	// kBitsPerMask bits starting at bit offset N.
	static constexpr int kTotalBytes = (kNumMasks + 64) / 8;
	uint8_t masks_[kTotalBytes];

private:
	bool GetBit(const uint8_t *data, uint64_t bit_pos) const {
		return (data[bit_pos / 8] >> (bit_pos % 8)) & 1;
	}
	void SetBit(uint8_t *data, uint64_t bit_pos) {
		data[bit_pos / 8] |= (1 << (bit_pos % 8));
	}

	uint64_t ROTL64(uint64_t x, int r) {
		return (x << r) | (x >> ((64 - r) & 63));
	}

	uint64_t GetMask(int bit_offset) {
		uint64_t value;
		std::memcpy(&value, masks_ + bit_offset / 8, sizeof(uint64_t));
		return (value >> (bit_offset % 8)) & kFullMask;
	}
};

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

	bool finalized_;

public:
	size_t Lookup(DataChunk &chunk, vector<uint64_t> &results);
	void Insert(DataChunk &chunk);

	uint32_t num_blocks_;
	uint32_t num_blocks_log;

	std::mutex insert_lock;
	uint64_t *blocks_;

public:
	// The columns that this BF applies, and the columns used to build this BF
	vector<ColumnBinding> column_bindings_applied_;
	vector<ColumnBinding> column_bindings_built_;
	vector<idx_t> BoundColsApplied;
	vector<idx_t> BoundColsBuilt;

private:
	size_t BloomFilterLookup(size_t num, uint64_t *BF_RESTRICT key, uint64_t *BF_RESTRICT bf,
	                         uint64_t *BF_RESTRICT out) const {
		for (size_t i = 0; i < num; i++) {
			uint32_t block = (key[i] >> (64 - num_blocks_log)) & (MAX_NUM_BLOCKS - 1);
			uint64_t mask = masks_.Mask(key[i]);
			out[i] = (bf[block] & mask) == mask;
		}
		return num;
	}

	void BloomFilterInsert(size_t num, uint64_t *BF_RESTRICT key, uint64_t *BF_RESTRICT bf) const {
		for (size_t i = 0; i < num; i++) {
			uint32_t block = (key[i] >> (64 - num_blocks_log)) & (MAX_NUM_BLOCKS - 1);
			uint64_t mask = masks_.Mask(key[i]);
			bf[block] |= mask;
		}
	}

	AllocatedData buf_;
	static BloomFilterMasks masks_;
};

} // namespace duckdb
