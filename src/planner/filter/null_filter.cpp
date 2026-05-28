#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

LegacyIsNullFilter::LegacyIsNullFilter() : TableFilter(TableFilterType::LEGACY_IS_NULL) {
}

unique_ptr<Expression> LegacyIsNullFilter::ToExpression(const Expression &column) const {
	return ExpressionFilter::CreateNullCheckExpression(column.Copy(), ExpressionType::OPERATOR_IS_NULL);
}

LegacyIsNotNullFilter::LegacyIsNotNullFilter() : TableFilter(TableFilterType::LEGACY_IS_NOT_NULL) {
}

unique_ptr<Expression> LegacyIsNotNullFilter::ToExpression(const Expression &column) const {
	return ExpressionFilter::CreateNullCheckExpression(column.Copy(), ExpressionType::OPERATOR_IS_NOT_NULL);
}

} // namespace duckdb
