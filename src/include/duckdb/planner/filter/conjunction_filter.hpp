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
	static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_OR;

public:
	ConjunctionOrFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

class ConjunctionAndFilter : public ConjunctionFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_AND;

public:
	ConjunctionAndFilter();

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
