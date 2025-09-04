

#include "duckdb/planner/filter/bloom_filter.hpp"
namespace duckdb {

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


idx_t CacheSectorizedBloomFilter::LookupHashes(Vector &hashes, SelectionVector &res_sel, const idx_t count) const {
	return BloomFilterLookup(reinterpret_cast<hash_t *>(hashes.GetData()), blocks, res_sel, count);
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