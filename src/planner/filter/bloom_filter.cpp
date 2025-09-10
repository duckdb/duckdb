

#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

string BloomFilter::ToString(const string &column_name) const {
	return column_name +  " IN BF(" + key_column_name + ")";
}

unique_ptr<Expression> BloomFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

idx_t CacheSectorizedBloomFilter::LookupHashes(Vector &hashes, SelectionVector &res_sel, const idx_t count) const {
	return BloomFilterLookup(reinterpret_cast<hash_t *>(hashes.GetData()), blocks, res_sel, count);
}

bool CacheSectorizedBloomFilter::LookupHash(hash_t hash) const {
	// Reinterpret the address of a value as a pointer to uint32_t
	const uint32_t *parts = reinterpret_cast<uint32_t *>(&hash);

	const uint32_t lower = parts[0];
	const uint32_t higher = parts[1];

	return LookupOne(lower, higher, blocks);
}

void CacheSectorizedBloomFilter::InsertHashes(Vector hashes, const idx_t count) {
	std::lock_guard<std::mutex> lock(insert_lock);
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
}

} // namespace duckdb
