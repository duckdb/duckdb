#include "duckdb/planner/filter/conjunction_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

LegacyConjunctionOrFilter::LegacyConjunctionOrFilter()
    : LegacyConjunctionFilter(TableFilterType::LEGACY_CONJUNCTION_OR) {
}

unique_ptr<Expression> LegacyConjunctionOrFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

LegacyConjunctionAndFilter::LegacyConjunctionAndFilter()
    : LegacyConjunctionFilter(TableFilterType::LEGACY_CONJUNCTION_AND) {
}

unique_ptr<Expression> LegacyConjunctionAndFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

} // namespace duckdb
