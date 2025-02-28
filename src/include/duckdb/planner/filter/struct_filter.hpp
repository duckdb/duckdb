//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/constant_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

class StructFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::STRUCT_EXTRACT;

public:
	StructFilter(idx_t child_idx, string child_name, unique_ptr<TableFilter> child_filter);

	//! The field index to filter on
	idx_t child_idx;

	//! The field name to filter on
	string child_name;

	//! The child filter
	unique_ptr<TableFilter> child_filter;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
