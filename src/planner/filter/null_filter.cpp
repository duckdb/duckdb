#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

unique_ptr<Expression> IsNullFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	return std::move(result);
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

unique_ptr<Expression> IsNotNullFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	return std::move(result);
}

} // namespace duckdb
