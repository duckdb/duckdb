#include "duckdb/planner/filter/conjunction_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

ConjunctionOrFilter::ConjunctionOrFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_OR) {
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

unique_ptr<Expression> ConjunctionAndFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

} // namespace duckdb
