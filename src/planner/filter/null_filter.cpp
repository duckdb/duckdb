#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

FilterPropagateResult IsNullFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("IsNullFilter");
}

string IsNullFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("IsNullFilter");
}

unique_ptr<TableFilter> IsNullFilter::Copy() const {
	TableFilter::ThrowDeprecated("IsNullFilter");
}

unique_ptr<Expression> IsNullFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	return std::move(result);
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

FilterPropagateResult IsNotNullFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("IsNotNullFilter");
}

string IsNotNullFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("IsNotNullFilter");
}

unique_ptr<TableFilter> IsNotNullFilter::Copy() const {
	TableFilter::ThrowDeprecated("IsNotNullFilter");
}

unique_ptr<Expression> IsNotNullFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	return std::move(result);
}

} // namespace duckdb
