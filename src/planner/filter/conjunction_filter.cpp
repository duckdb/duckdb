#include "duckdb/planner/filter/conjunction_filter.hpp"

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

} // namespace duckdb
