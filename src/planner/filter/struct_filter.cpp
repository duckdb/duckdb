#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"

namespace duckdb {

LegacyStructFilter::LegacyStructFilter(idx_t child_idx_p, string child_name_p, unique_ptr<TableFilter> child_filter_p)
    : TableFilter(TableFilterType::LEGACY_STRUCT_EXTRACT), child_idx(child_idx_p), child_name(std::move(child_name_p)),
      child_filter(std::move(child_filter_p)) {
}

unique_ptr<Expression> LegacyStructFilter::ToExpression(const Expression &column) const {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_idx + 1))));

	BoundScalarFunction bound_func(GetExtractAtFunction());
	bound_func.SetReturnType(StructType::GetChildType(column.GetReturnType(), child_idx));

	auto child = make_uniq<BoundFunctionExpression>(std::move(bound_func), std::move(arguments),
	                                                StructExtractAtFun::GetBindData(child_idx));
	return child_filter->ToExpression(*child);
}
} // namespace duckdb
