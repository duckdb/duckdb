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

void BloomFilter::Initialize(ClientContext &context_p, uint32_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	uint32_t min_bits = std::max<uint32_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	num_blocks_ = std::min(CeilPowerOfTwo(min_bits) >> LOG_BLOCK_SIZE, MAX_NUM_BLOCKS);
	num_blocks_log = static_cast<uint32_t>(std::log2(num_blocks_));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(num_blocks_ * sizeof(uint32_t));
	blocks_ = reinterpret_cast<uint32_t *>(buf_.get());
	std::fill_n(blocks_, num_blocks_, 0);
}

size_t BloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, BoundColsApplied);
	BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks_, results.data());
	return count;
}

void BloomFilter::Insert(DataChunk &chunk) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, BoundColsBuilt);
	std::lock_guard<std::mutex> lock(insert_lock);
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks_);
}
} // namespace duckdb
