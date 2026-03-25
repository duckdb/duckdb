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

FilterPropagateResult InFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("InFilter");
}

string InFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("InFilter");
}

unique_ptr<Expression> InFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	for (auto &val : values) {
		result->children.push_back(make_uniq<BoundConstantExpression>(val));
	}
	return std::move(result);
}

bool InFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("InFilter");
}

unique_ptr<TableFilter> InFilter::Copy() const {
	TableFilter::ThrowDeprecated("InFilter");
}

} // namespace duckdb
