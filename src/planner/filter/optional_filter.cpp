#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"

namespace duckdb {

OptionalFilter::OptionalFilter(unique_ptr<TableFilter> filter)
    : TableFilter(TableFilterType::OPTIONAL_FILTER), child_filter(std::move(filter)) {
}

FilterPropagateResult OptionalFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

string OptionalFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

unique_ptr<Expression> OptionalFilter::ToExpression(const Expression &column) const {
	auto func = OptionalFilterScalarFun::GetFunction(column.return_type);
	auto child_expr = child_filter ? child_filter->ToExpression(column) : nullptr;
	auto bind_data = make_uniq<OptionalFilterFunctionData>(std::move(child_expr));
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

void OptionalFilter::FiltersNullValues(const LogicalType &type, bool &filters_nulls, bool &filters_valid_values,
                                       TableFilterState &filter_state) const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

unique_ptr<TableFilterState> OptionalFilter::InitializeState(ClientContext &context) const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

idx_t OptionalFilter::FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                      TableFilterState &filter_state, const idx_t scan_count,
                                      idx_t &approved_tuple_count) const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

unique_ptr<TableFilter> OptionalFilter::Copy() const {
	TableFilter::ThrowDeprecated("OptionalFilter");
}

} // namespace duckdb
