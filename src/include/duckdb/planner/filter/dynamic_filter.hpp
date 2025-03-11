
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

namespace duckdb {

struct DynamicFilterData {
	mutex lock;
	unique_ptr<TableFilter> filter;
	bool initialized = false;

	void SetValue(Value val);
	void Reset();
};

class DynamicFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::DYNAMIC_FILTER;

public:
	DynamicFilter();
	explicit DynamicFilter(shared_ptr<DynamicFilterData> filter_data);

	//! The shared, dynamic filter data
	shared_ptr<DynamicFilterData> filter_data;

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
