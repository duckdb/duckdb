#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"

namespace duckdb {

StructFilter::StructFilter(idx_t child_idx_p, string child_name_p, unique_ptr<TableFilter> child_filter_p)
    : TableFilter(TableFilterType::STRUCT_EXTRACT), child_idx(child_idx_p), child_name(std::move(child_name_p)),
      child_filter(std::move(child_filter_p)) {
}

unique_ptr<Expression> StructFilter::ToExpression(const Expression &column) const {
	auto &child_type = StructType::GetChildType(column.return_type, child_idx);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(column.Copy());
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_idx + 1))));
	auto child = make_uniq<BoundFunctionExpression>(child_type, GetExtractAtFunction(), std::move(arguments),
	                                                StructExtractAtFun::GetBindData(child_idx));
	return child_filter->ToExpression(*child);
}
} // namespace duckdb
