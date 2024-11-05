
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/optional_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

class OptionalFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::OPTIONAL_FILTER;

public:
	OptionalFilter();

	string ToString(const string &column_name) override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

public:
	// optional child filters
	unique_ptr<TableFilter> child_filter;
};

} // namespace duckdb
