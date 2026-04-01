#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

OptionalFilter::OptionalFilter(unique_ptr<TableFilter> filter)
    : TableFilter(TableFilterType::OPTIONAL_FILTER), child_filter(std::move(filter)) {
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

} // namespace duckdb
