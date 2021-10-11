//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/conjunction_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ConjunctionOrFilter : public TableFilter {
public:
	ConjunctionOrFilter();

	//! The filters to OR together
	vector<unique_ptr<TableFilter>> child_filters;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
};

class ConjunctionAndFilter : public TableFilter {
public:
	ConjunctionAndFilter();

	//! The filters to OR together
	vector<unique_ptr<TableFilter>> child_filters;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
};

} // namespace duckdb
