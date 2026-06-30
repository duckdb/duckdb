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
class LegacyIsNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_IS_NULL;

public:
	LegacyIsNullFilter();

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacyIsNotNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_IS_NOT_NULL;

public:
	LegacyIsNotNullFilter();

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
