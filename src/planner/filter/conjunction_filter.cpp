#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

ConjunctionOrFilter::ConjunctionOrFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_OR) {
}

FilterPropagateResult ConjunctionOrFilter::CheckStatistics(BaseStatistics &stats) {
	// the OR filter is true if ANY of the children is true
	D_ASSERT(!child_filters.empty());
	for (auto &filter : child_filters) {
		auto prune_result = filter->CheckStatistics(stats);
		if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

string ConjunctionOrFilter::ToString(const string &column_name) {
	string result;
	for (idx_t i = 0; i < child_filters.size(); i++) {
		if (i > 0) {
			result += " OR ";
		}
		result += child_filters[i]->ToString(column_name);
	}
	return result;
}

bool ConjunctionOrFilter::Equals(const TableFilter &other_p) const {
	if (!ConjunctionFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ConjunctionOrFilter>();
	if (other.child_filters.size() != child_filters.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.child_filters.size(); i++) {
		if (!child_filters[i]->Equals(*other.child_filters[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<TableFilter> ConjunctionOrFilter::Copy() const {
	auto result = make_uniq<ConjunctionOrFilter>();
	for (auto &filter : child_filters) {
		result->child_filters.push_back(filter->Copy());
	}
	return std::move(result);
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

FilterPropagateResult ConjunctionAndFilter::CheckStatistics(BaseStatistics &stats) {
	// the AND filter is true if ALL of the children is true
	D_ASSERT(!child_filters.empty());
	auto result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
	for (auto &filter : child_filters) {
		auto prune_result = filter->CheckStatistics(stats);
		if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else if (prune_result != result) {
			result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	return result;
}

string ConjunctionAndFilter::ToString(const string &column_name) {
	string result;
	for (idx_t i = 0; i < child_filters.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += child_filters[i]->ToString(column_name);
	}
	return result;
}

bool ConjunctionAndFilter::Equals(const TableFilter &other_p) const {
	if (!ConjunctionFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ConjunctionAndFilter>();
	if (other.child_filters.size() != child_filters.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.child_filters.size(); i++) {
		if (!child_filters[i]->Equals(*other.child_filters[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<TableFilter> ConjunctionAndFilter::Copy() const {
	auto result = make_uniq<ConjunctionAndFilter>();
	for (auto &filter : child_filters) {
		result->child_filters.push_back(filter->Copy());
	}
	return std::move(result);
}

unique_ptr<Expression> ConjunctionAndFilter::ToExpression(const Expression &column) const {
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &filter : child_filters) {
		conjunction->children.push_back(filter->ToExpression(column));
	}
	return std::move(conjunction);
}

} // namespace duckdb
