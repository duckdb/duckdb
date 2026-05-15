//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/in_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacyInFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_IN_FILTER;

public:
	explicit LegacyInFilter(vector<Value> values);

	vector<Value> values;

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
