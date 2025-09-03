

#include "duckdb/planner/filter/bloom_filter.hpp"
namespace duckdb {


static idx_t CeilPowerOfTwo(idx_t n) {
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

static Vector HashColumns(DataChunk &chunk, const vector<idx_t> &cols) {
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

void CacheSectorizedBloomFilter::Initialize(ClientContext &context_p, idx_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(CeilPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS); // todo: There is a ddb function for that
	num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, num_sectors, 0);
}

idx_t CacheSectorizedBloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied) const {
	const idx_t count = chunk.size();
	const Vector hashes = HashColumns(chunk, bound_cols_applied);
	return BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks, results.data());
}

idx_t CacheSectorizedBloomFilter::LookupHashes(Vector &hashes,const idx_t count, vector<uint32_t> &results) const {
	return BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks, results.data());
}

void CacheSectorizedBloomFilter::InsertKeys(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	Vector hashes = HashColumns(chunk, bound_cols_built);
	InsertHashes(hashes, chunk.size());
}

void CacheSectorizedBloomFilter::InsertHashes(Vector hashes, const idx_t count) {
	std::lock_guard<std::mutex> lock(insert_lock);
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
}

}