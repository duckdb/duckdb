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

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class ConjunctionFilter : public TableFilter {
public:
	explicit ConjunctionFilter(TableFilterType filter_type_p) : TableFilter(filter_type_p) {
	}

	~ConjunctionFilter() override {
	}

	//! The filters of this conjunction
	vector<unique_ptr<TableFilter>> child_filters;

public:
	bool Equals(const TableFilter &other) const override {
		TableFilter::ThrowDeprecated("ConjunctionFilter");
	}
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class ConjunctionOrFilter : public ConjunctionFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_OR;

public:
	ConjunctionOrFilter();
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class ConjunctionAndFilter : public ConjunctionFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_AND;

public:
	ConjunctionAndFilter();

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
