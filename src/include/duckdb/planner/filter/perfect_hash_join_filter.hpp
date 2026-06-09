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

namespace duckdb {

class PerfectHashJoinExecutor;

//! DEPRECATED - only preserved for backwards-compatible expression conversion
class LegacyPerfectHashJoinFilter final : public TableFilter {
public:
	static constexpr auto TYPE = TableFilterType::LEGACY_PERFECT_HASH_JOIN_FILTER;

public:
	LegacyPerfectHashJoinFilter(optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor,
	                            const string &key_column_name, const LogicalType &key_type_p);

private:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

private:
	optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor;
	const string key_column_name;
	const LogicalType key_type;
};

} // namespace duckdb
