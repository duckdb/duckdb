//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
class LegacySelectivityOptionalFilter final : public LegacyOptionalFilter {
public:
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter, SelectivityOptionalFilterType type);
	LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold,
	                                idx_t n_vectors_to_check);

public:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
