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

class InFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IN_FILTER;

public:
	explicit InFilter(vector<Value> values);

	vector<Value> values;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
