#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

BloomFilter::BloomFilter(shared_ptr<JoinBloomFilter> join_bloom_filter, const vector<column_t> column_ids, const PhysicalHashJoin &hj, ClientContext &client_context) : TableFilter(TableFilterType::BLOOM_FILTER), bf(std::move(join_bloom_filter)), column_ids(std::move(column_ids)), hj(hj), client_context(client_context) {
}

FilterPropagateResult BloomFilter::CheckStatistics(BaseStatistics &stats) {
    // No partition pruning with current Bloom-filter implementation possible.
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string BloomFilter::ToString(const string &column_name) {
	return "BLOOM_FILTER(" + column_name + ")";
}

unique_ptr<Expression> BloomFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	// TODO
	return std::move(result);
}

bool BloomFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
    // TODO
}

unique_ptr<TableFilter> BloomFilter::Copy() const {
	return make_uniq<BloomFilter>(bf, column_ids, hj, client_context);
}

} // namespace duckdb
