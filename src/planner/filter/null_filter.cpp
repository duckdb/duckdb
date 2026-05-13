#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

FilterPropagateResult IsNullFilter::CheckStatistics(BaseStatistics &stats) const {
	throw InternalException("IsNullFilter::CheckStatistics should not be called: IsNullFilters should be converted "
	                        "to ExpressionFilters before statistics checking");
}

string IsNullFilter::ToString(const string &column_name) const {
	throw InternalException("IsNullFilter::ToString should not be called: IsNullFilters should be converted to "
	                        "ExpressionFilters before rendering");
}

bool IsNullFilter::Equals(const TableFilter &other_p) const {
	throw InternalException("IsNullFilter::Equals should not be called: IsNullFilters should be converted to "
	                        "ExpressionFilters before equality checking");
}

unique_ptr<TableFilter> IsNullFilter::Copy() const {
	throw InternalException("IsNullFilter::Copy should not be called: IsNullFilters should be converted to "
	                        "ExpressionFilters before copying");
}

unique_ptr<Expression> IsNullFilter::ToExpression(const Expression &column) const {
	return ExpressionFilter::CreateNullCheckExpression(column.Copy(), ExpressionType::OPERATOR_IS_NULL);
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

FilterPropagateResult IsNotNullFilter::CheckStatistics(BaseStatistics &stats) const {
	throw InternalException("IsNotNullFilter::CheckStatistics should not be called: IsNotNullFilters should be "
	                        "converted to ExpressionFilters before statistics checking");
}

string IsNotNullFilter::ToString(const string &column_name) const {
	throw InternalException("IsNotNullFilter::ToString should not be called: IsNotNullFilters should be converted "
	                        "to ExpressionFilters before rendering");
}

bool IsNotNullFilter::Equals(const TableFilter &other_p) const {
	throw InternalException("IsNotNullFilter::Equals should not be called: IsNotNullFilters should be converted to "
	                        "ExpressionFilters before equality checking");
}

unique_ptr<TableFilter> IsNotNullFilter::Copy() const {
	throw InternalException("IsNotNullFilter::Copy should not be called: IsNotNullFilters should be converted to "
	                        "ExpressionFilters before copying");
}

unique_ptr<Expression> IsNotNullFilter::ToExpression(const Expression &column) const {
	return ExpressionFilter::CreateNullCheckExpression(column.Copy(), ExpressionType::OPERATOR_IS_NOT_NULL);
}

} // namespace duckdb
