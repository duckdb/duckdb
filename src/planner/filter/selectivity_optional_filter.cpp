//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"

#include "duckdb/planner/table_filter_state.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

SelectivityOptionalFilter::SelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold,
                                                     idx_t n_vectors_to_check)
    : OptionalFilter(std::move(filter)), selectivity_threshold(selectivity_threshold),
      n_vectors_to_check(n_vectors_to_check) {
}

SelectivityOptionalFilter::SelectivityOptionalFilter(unique_ptr<TableFilter> filter, SelectivityOptionalFilterType type)
    : OptionalFilter(std::move(filter)) {
	GetThresholdAndVectorsToCheck(type, selectivity_threshold, n_vectors_to_check);
}

unique_ptr<Expression> SelectivityOptionalFilter::ToExpression(const Expression &column) const {
	auto func = SelectivityOptionalFilterScalarFun::GetFunction(column.return_type);
	auto child_expr = child_filter ? child_filter->ToExpression(column) : nullptr;
	auto bind_data = make_uniq<SelectivityOptionalFilterFunctionData>(std::move(child_expr), selectivity_threshold,
	                                                                  n_vectors_to_check);
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

void SelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	OptionalFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", n_vectors_to_check);
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = unique_ptr<SelectivityOptionalFilter>(new SelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	deserializer.ReadPropertyWithDefault<float>(201, "selectivity_threshold", result->selectivity_threshold);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "n_vectors_to_check", result->n_vectors_to_check);
	return std::move(result);
}

} // namespace duckdb
