//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

SelectivityOptionalFilter::SelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                     const float selectivity_threshold_p,
                                                     const idx_t n_vectors_to_check_p)
    : TableFilter(TYPE), selectivity_threshold(selectivity_threshold_p), n_vectors_to_check(n_vectors_to_check_p),
      child_filter(std::move(filter)) {
}

FilterPropagateResult SelectivityOptionalFilter::CheckStatistics(BaseStatistics &stats) const {
	// TODO: A potential optimization would be to pause the filter for this row group if the stats return always true,
	//		 but this needs to happen thread local, as other threads scan other row groups
	return child_filter->CheckStatistics(stats);
}

unique_ptr<Expression> SelectivityOptionalFilter::ToExpression(const Expression &column) const {
	return child_filter->ToExpression(column);
}

void SelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", child_filter);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", n_vectors_to_check);
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SelectivityOptionalFilter>(new SelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	deserializer.ReadPropertyWithDefault<float>(201, "selectivity_threshold", result->selectivity_threshold);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "n_vectors_to_check", result->n_vectors_to_check);
	return std::move(result);
}

string SelectivityOptionalFilter::ToString(const string &column_name) const {
	const auto child_string = child_filter ? child_filter->ToString(column_name) : "NULL";
	return string("s_optional: ") + child_string;
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Copy() const {
	auto copy = make_uniq<SelectivityOptionalFilter>(child_filter->Copy(), selectivity_threshold, n_vectors_to_check);
	return duckdb::unique_ptr_cast<SelectivityOptionalFilter, TableFilter>(std::move(copy));
}

} // namespace duckdb
