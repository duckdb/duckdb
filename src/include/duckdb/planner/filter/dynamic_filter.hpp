
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/dynamic_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class Expression;
class DynamicFilterData;

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacyDynamicFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_DYNAMIC_FILTER;

public:
	LegacyDynamicFilter();
	explicit LegacyDynamicFilter(shared_ptr<DynamicFilterData> filter_data);

	//! The shared, dynamic filter data
	shared_ptr<DynamicFilterData> filter_data;

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
