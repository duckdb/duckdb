#include "duckdb/planner/filter/conjunction_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

ConjunctionOrFilter::ConjunctionOrFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_OR) {
}

FilterPropagateResult ConjunctionOrFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("ConjunctionOrFilter");
}

string ConjunctionOrFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("ConjunctionOrFilter");
}

bool ConjunctionOrFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("ConjunctionOrFilter");
}

unique_ptr<TableFilter> ConjunctionOrFilter::Copy() const {
	TableFilter::ThrowDeprecated("ConjunctionOrFilter");
}

unique_ptr<Expression> ConjunctionOrFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

ConjunctionAndFilter::ConjunctionAndFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_AND) {
}

FilterPropagateResult ConjunctionAndFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("ConjunctionAndFilter");
}

string ConjunctionAndFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("ConjunctionAndFilter");
}

bool ConjunctionAndFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("ConjunctionAndFilter");
}

unique_ptr<TableFilter> ConjunctionAndFilter::Copy() const {
	TableFilter::ThrowDeprecated("ConjunctionAndFilter");
}

unique_ptr<Expression> ConjunctionAndFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

} // namespace duckdb
