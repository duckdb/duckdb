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
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
};

class IsNotNullFilter : public TableFilter {
public:
	IsNotNullFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
};

} // namespace duckdb
