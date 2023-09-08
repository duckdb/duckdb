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

class IsNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IS_NULL;

public:
	IsNullFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	void FormatSerialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> FormatDeserialize(Deserializer &deserializer);
};

class IsNotNullFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IS_NOT_NULL;

public:
	IsNotNullFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	void FormatSerialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> FormatDeserialize(Deserializer &deserializer);
};

} // namespace duckdb
