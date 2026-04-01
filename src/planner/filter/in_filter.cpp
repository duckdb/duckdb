#include "duckdb/planner/filter/in_filter.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

InFilter::InFilter(vector<Value> values_p) : TableFilter(TableFilterType::IN_FILTER), values(std::move(values_p)) {
	for (auto &val : values) {
		if (val.IsNull()) {
			throw InternalException("InFilter constant cannot be NULL - use IsNullFilter instead");
		}
	}
	for (idx_t i = 1; i < values.size(); i++) {
		if (values[0].type() != values[i].type()) {
			throw InternalException("InFilter constants must all have the same type");
		}
	}
	if (values.empty()) {
		throw InternalException("InFilter constants cannot be empty");
	}
}

unique_ptr<Expression> InFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	for (auto &val : values) {
		result->children.push_back(make_uniq<BoundConstantExpression>(val));
	}
	return std::move(result);
}

} // namespace duckdb
