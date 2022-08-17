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
class ConjunctionFilter : public TableFilter {
public:
	ConjunctionFilter(TableFilterType filter_type_p) : TableFilter(filter_type_p) {
	}

	virtual ~ConjunctionFilter() {
	}

	//! The filters of this conjunction
	vector<unique_ptr<TableFilter>> child_filters;

public:
	virtual FilterPropagateResult CheckStatistics(BaseStatistics &stats) = 0;
	virtual string ToString(const string &column_name) = 0;

	virtual bool Equals(const TableFilter &other) const {
		return TableFilter::Equals(other);
	}
};

class ConjunctionOrFilter : public ConjunctionFilter {
public:
	ConjunctionOrFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<TableFilter> Deserialize(FieldReader &source);
};

class ConjunctionAndFilter : public ConjunctionFilter {
public:
	ConjunctionAndFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<TableFilter> Deserialize(FieldReader &source);
};

} // namespace duckdb
