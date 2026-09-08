#include "duckdb/planner/filter/in_filter.hpp"

#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

LegacyInFilter::LegacyInFilter(vector<Value> values_p)
    : TableFilter(TableFilterType::LEGACY_IN_FILTER), values(std::move(values_p)) {
	for (auto &val : values) {
		if (val.IsNull()) {
			throw InternalException("LegacyInFilter constant cannot be NULL - use IsNullFilter instead");
		}
	}
	for (idx_t i = 1; i < values.size(); i++) {
		if (values[0].type() != values[i].type()) {
			throw InternalException("LegacyInFilter constants must all have the same type");
		}
	}
	if (values.empty()) {
		throw InternalException("LegacyInFilter constants cannot be empty");
	}
}

unique_ptr<Expression> LegacyInFilter::ToExpression(const Expression &column) const {
	return ExpressionFilter::CreateInExpression(column.Copy(), values);
}

} // namespace duckdb
