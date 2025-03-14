#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <random>
#include <cmath>
#include <iostream>

namespace duckdb {
namespace {
static uint32_t CeilPowerOfTwo(uint32_t n) {
	if (n <= 1) {
		return 1;
	}
	n--;
	n |= (n >> 1);
	n |= (n >> 2);
	n |= (n >> 4);
	n |= (n >> 8);
	n |= (n >> 16);
	return n + 1;
}

static Vector HashColumns(DataChunk &chunk, vector<idx_t> &cols) {
	auto count = chunk.size();
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(chunk.data[cols[0]], hashes, count);
	for (size_t j = 1; j < cols.size(); j++) {
		VectorOperations::CombineHash(hashes, chunk.data[cols[j]], count);
	}

	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		hashes.Flatten(count);
	}

	return hashes;
}
} // namespace

BloomFilterMasks::BloomFilterMasks() {
	std::seed_seq seed {0, 0, 0, 0, 0, 0, 0, 0};
	std::mt19937 re(seed);
	std::uniform_int_distribution<uint64_t> rd;

	auto random = [&re, &rd](uint64_t min_value, uint64_t max_value) {
		return min_value + rd(re) % (max_value - min_value + 1);
	};

	memset(masks_, 0, kTotalBytes);

	// Prepare the first mask
	uint64_t num_bits_set = random(kMinBitsSet, kMaxBitsSet);
	for (uint64_t i = 0; i < num_bits_set; ++i) {
		while (true) {
			uint64_t bit_pos = random(0, kBitsPerMask - 1);
			if (!GetBit(masks_, bit_pos)) {
				SetBit(masks_, bit_pos);
				break;
			}
		}
	}

	uint64_t num_bits_total = kNumMasks + kBitsPerMask - 1;
	for (uint64_t i = kBitsPerMask; i < num_bits_total; ++i) {
		int bit_leaving = GetBit(masks_, i - kBitsPerMask) ? 1 : 0;
		if (bit_leaving == 1 && num_bits_set == kMinBitsSet) {
			SetBit(masks_, i);
			continue;
		}

		if (bit_leaving == 0 && num_bits_set == kMaxBitsSet) {
			continue;
		}

		if (random(0, kBitsPerMask * 2 - 1) < kMinBitsSet + kMaxBitsSet) {
			SetBit(masks_, i);
			if (bit_leaving == 0) {
				++num_bits_set;
			}
		} else {
			if (bit_leaving == 1) {
				--num_bits_set;
			}
		}
	}
}

BloomFilterMasks BloomFilter::masks_;

void BloomFilter::Initialize(ClientContext &context_p, uint32_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	uint32_t min_bits = std::max<uint32_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	num_blocks_ = std::min(CeilPowerOfTwo(min_bits) >> LOG_BLOCK_SIZE, MAX_NUM_BLOCKS);
	num_blocks_log = static_cast<uint32_t>(std::log2(num_blocks_));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(num_blocks_ * sizeof(uint64_t));
	blocks_ = reinterpret_cast<uint64_t *>(buf_.get());
	std::fill_n(blocks_, num_blocks_, 0);
}

size_t BloomFilter::Lookup(DataChunk &chunk, vector<uint64_t> &results) {
	size_t count = chunk.size();
	Vector hashes = HashColumns(chunk, BoundColsApplied);
	BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks_, results.data());
	return count;
}

void BloomFilter::Insert(DataChunk &chunk) {
	Vector hashes = HashColumns(chunk, BoundColsBuilt);

	// vectorized insert
	std::lock_guard<std::mutex> lock(insert_lock);
	BloomFilterInsert(chunk.size(), reinterpret_cast<uint64_t *>(hashes.GetData()), blocks_);
}
} // namespace duckdb
