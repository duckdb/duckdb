#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

LegacyConstantFilter::LegacyConstantFilter(ExpressionType comparison_type_p, Value constant_p)
    : TableFilter(TableFilterType::LEGACY_CONSTANT_COMPARISON), comparison_type(comparison_type_p),
      constant(std::move(constant_p)) {
	if (constant.IsNull()) {
		throw InternalException("LegacyConstantFilter constant cannot be NULL - use IsNullFilter instead");
	}
}

unique_ptr<Expression> LegacyConstantFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(constant);
	auto result = BoundComparisonExpression::Create(comparison_type, column.Copy(), std::move(bound_constant));
	return result;
}

} // namespace duckdb
