//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/null_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class IsNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IS_NULL;

public:
	IsNullFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class IsNotNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IS_NOT_NULL;

public:
	IsNotNullFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
