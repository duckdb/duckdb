//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "duckdb/planner/table_filter_state.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

LegacySelectivityOptionalFilter::LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                                 float selectivity_threshold, idx_t n_vectors_to_check)
    : LegacyOptionalFilter(std::move(filter)), selectivity_threshold(selectivity_threshold),
      n_vectors_to_check(n_vectors_to_check) {
}

LegacySelectivityOptionalFilter::LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                                 SelectivityOptionalFilterType type) {
	float threshold;
	idx_t vectors_to_check;
	GetThresholdAndVectorsToCheck(type, threshold, vectors_to_check);
	child_filter = std::move(filter);
	selectivity_threshold = threshold;
	n_vectors_to_check = vectors_to_check;
}

void LegacySelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	LegacyOptionalFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", n_vectors_to_check);
}

unique_ptr<TableFilter> LegacySelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = unique_ptr<LegacySelectivityOptionalFilter>(new LegacySelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	deserializer.ReadPropertyWithDefault<float>(201, "selectivity_threshold", result->selectivity_threshold);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "n_vectors_to_check", result->n_vectors_to_check);
	return std::move(result);
}

unique_ptr<Expression> LegacySelectivityOptionalFilter::ToExpression(const Expression &column) const {
	auto child_expr = child_filter ? child_filter->ToExpression(column) : nullptr;
	return CreateSelectivityOptionalFilterExpression(std::move(child_expr), column.GetReturnType(),
	                                                 selectivity_threshold, n_vectors_to_check);
}

} // namespace duckdb
