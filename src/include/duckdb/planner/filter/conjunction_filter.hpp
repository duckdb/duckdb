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
class LegacyConjunctionFilter : public TableFilter {
public:
	explicit LegacyConjunctionFilter(TableFilterType filter_type_p) : TableFilter(filter_type_p) {
	}

	~LegacyConjunctionFilter() override {
	}

	//! The filters of this conjunction
	vector<unique_ptr<TableFilter>> child_filters;
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacyConjunctionOrFilter : public LegacyConjunctionFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_CONJUNCTION_OR;

public:
	LegacyConjunctionOrFilter();
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacyConjunctionAndFilter : public LegacyConjunctionFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::LEGACY_CONJUNCTION_AND;

public:
	LegacyConjunctionAndFilter();

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
