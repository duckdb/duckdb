//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/perfect_hash_join_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class PerfectHashJoinExecutor;

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class PerfectHashJoinFilter final : public TableFilter {
public:
	static constexpr auto TYPE = TableFilterType::PERFECT_HASH_JOIN_FILTER;

public:
	explicit PerfectHashJoinFilter(const string &key_column_name);

private:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

private:
	const string key_column_name;
};

} // namespace duckdb
