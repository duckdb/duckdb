#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

#include <random>
#include <cmath>

namespace duckdb {
BloomFilterMasks BlockedBloomFilter::masks_;

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
		for (;;) {
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

void BlockedBloomFilter::Initialize(ClientContext &context_p, size_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	uint64_t min_bits = std::max<uint64_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	uint64_t total_bits = 1LL << static_cast<int64_t>(std::ceil(std::log2(static_cast<double>(min_bits))));
	num_blocks_ = total_bits >> LOG_BLOCK_SIZE;

	buf_ = buffer_manager->GetBufferAllocator().Allocate(num_blocks_ * sizeof(uint64_t));
	blocks_ = reinterpret_cast<std::atomic<uint64_t> *>(buf_.get());
	std::fill_n(blocks_, num_blocks_, 0);
}

void BlockedBloomFilter::InsertHashedData(DataChunk &chunk, const vector<idx_t> &cols) {
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(chunk.data[cols[0]], hashes, chunk.size());

	for (size_t j = 1; j < cols.size(); j++) {
		VectorOperations::CombineHash(hashes, chunk.data[cols[j]], chunk.size());
	}

	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		hashes.Flatten(chunk.size());
	}

	// Loop through all hash values and insert them one by one
	auto hash_data = reinterpret_cast<hash_t *>(hashes.GetData());
	for (size_t i = 0; i < chunk.size(); i++) {
		Insert(hash_data[i]);
	}
}
} // namespace duckdb
