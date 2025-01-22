//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/in_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/join_bloom_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {

class BloomFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::BLOOM_FILTER;

public:
	BloomFilter(shared_ptr<JoinBloomFilter> join_bloom_filter, const vector<column_t> column_ids, const PhysicalHashJoin &hj, ClientContext &client_context);

    shared_ptr<JoinBloomFilter> bf;
    const vector<column_t> column_ids;
    const PhysicalHashJoin &hj;
    ClientContext &client_context;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
