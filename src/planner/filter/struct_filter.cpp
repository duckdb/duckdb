#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"

namespace duckdb {

StructFilter::StructFilter(idx_t child_idx_p, string child_name_p, unique_ptr<TableFilter> child_filter_p)
    : TableFilter(TableFilterType::STRUCT_EXTRACT), child_idx(child_idx_p), child_name(std::move(child_name_p)),
      child_filter(std::move(child_filter_p)) {
}

FilterPropagateResult StructFilter::CheckStatistics(BaseStatistics &stats) const {
	throw InternalException("StructFilter::CheckStatistics should not be called: StructFilters should be converted "
	                        "to ExpressionFilters before statistics checking");
}

string StructFilter::ToString(const string &column_name) const {
	throw InternalException("StructFilter::ToString should not be called: StructFilters should be converted to "
	                        "ExpressionFilters before rendering");
}

bool StructFilter::Equals(const TableFilter &other_p) const {
	throw InternalException("StructFilter::Equals should not be called: StructFilters should be converted to "
	                        "ExpressionFilters before equality checking");
}

unique_ptr<TableFilter> StructFilter::Copy() const {
	throw InternalException("StructFilter::Copy should not be called: StructFilters should be converted to "
	                        "ExpressionFilters before copying");
}

unique_ptr<Expression> StructFilter::ToExpression(const Expression &column) const {
	auto &child_type = StructType::GetChildType(column.GetReturnType(), child_idx);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_idx + 1))));

	BoundScalarFunction bound_func(GetExtractAtFunction());

	auto child = make_uniq<BoundFunctionExpression>(child_type, std::move(bound_func), std::move(arguments),
	                                                StructExtractAtFun::GetBindData(child_idx));
	return child_filter->ToExpression(*child);
}
} // namespace duckdb
