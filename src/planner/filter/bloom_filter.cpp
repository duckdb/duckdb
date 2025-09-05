

#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

unique_ptr<Expression> BloomFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

idx_t CacheSectorizedBloomFilter::LookupHashes(Vector &hashes, SelectionVector &res_sel, const idx_t count) const {
	return BloomFilterLookup(reinterpret_cast<hash_t *>(hashes.GetData()), blocks, res_sel, count);
}

void CacheSectorizedBloomFilter::InsertHashes(Vector hashes, const idx_t count) {
	std::lock_guard<std::mutex> lock(insert_lock);
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
}

} // namespace duckdb
