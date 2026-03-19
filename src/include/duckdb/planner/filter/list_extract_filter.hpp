//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/list_extract_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class ListExtractFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LIST_EXTRACT;

public:
	ListExtractFilter(Value child_selector, unique_ptr<TableFilter> child_filter);

	//! The element selector (integer index for LIST, or key for MAP)
	Value child_selector;
	//! The child filter to apply to the extracted element
	unique_ptr<TableFilter> child_filter;

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
