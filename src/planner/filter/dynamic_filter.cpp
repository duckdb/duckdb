#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

DynamicFilter::DynamicFilter() : TableFilter(TableFilterType::DYNAMIC_FILTER) {
}

DynamicFilter::DynamicFilter(shared_ptr<DynamicFilterData> filter_data_p)
    : TableFilter(TableFilterType::DYNAMIC_FILTER), filter_data(std::move(filter_data_p)) {
}

unique_ptr<Expression> DynamicFilter::ToExpression(const Expression &column) const {
	auto func = DynamicFilterScalarFun::GetFunction(column.return_type);
	auto bind_data = make_uniq<DynamicFilterFunctionData>(filter_data);
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

} // namespace duckdb
