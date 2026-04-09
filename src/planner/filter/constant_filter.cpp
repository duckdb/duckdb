#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

ConstantFilter::ConstantFilter(ExpressionType comparison_type_p, Value constant_p)
    : TableFilter(TableFilterType::CONSTANT_COMPARISON), comparison_type(comparison_type_p),
      constant(std::move(constant_p)) {
	if (constant.IsNull()) {
		throw InternalException("ConstantFilter constant cannot be NULL - use IsNullFilter instead");
	}
}

FilterPropagateResult ConstantFilter::CheckStatistics(BaseStatistics &stats) const {
	throw InternalException("ConstantFilter::CheckStatistics should not be called: ConstantFilters should be converted "
	                        "to ExpressionFilters before statistics checking");
}

string ConstantFilter::ToString(const string &column_name) const {
	throw InternalException("ConstantFilter::ToString should not be called: ConstantFilters should be converted to "
	                        "ExpressionFilters before rendering");
}

unique_ptr<Expression> ConstantFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(constant);
	auto result = make_uniq<BoundComparisonExpression>(comparison_type, column.Copy(), std::move(bound_constant));
	return std::move(result);
}

bool ConstantFilter::Equals(const TableFilter &other_p) const {
	throw InternalException("ConstantFilter::Equals should not be called: ConstantFilters should be converted to "
	                        "ExpressionFilters before equality checking");
}

unique_ptr<TableFilter> ConstantFilter::Copy() const {
	throw InternalException("ConstantFilter::Copy should not be called: ConstantFilters should be converted to "
	                        "ExpressionFilters before copying");
}

} // namespace duckdb
