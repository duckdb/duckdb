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
	IsNullFilter();

public:
	unique_ptr<TableFilter> Copy() override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<TableFilter> Deserialize(FieldReader &source);
};

class IsNotNullFilter : public TableFilter {
public:
	IsNotNullFilter();

public:
	unique_ptr<TableFilter> Copy() override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<TableFilter> Deserialize(FieldReader &source);
};

} // namespace duckdb
