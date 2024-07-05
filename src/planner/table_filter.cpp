#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"

namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = entry->second->Cast<ConjunctionAndFilter>();
			and_filter.child_filters.push_back(std::move(filter));
		} else {
			auto and_filter = make_uniq<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[column_index] = std::move(and_filter);
		}
	}
}

void DynamicTableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	lock_guard<mutex> l(lock);
	if (!filters) {
		filters = make_uniq<TableFilterSet>();
	}
	filters->PushFilter(column_index, std::move(filter));
}

bool DynamicTableFilterSet::HasFilters() const {
	lock_guard<mutex> l(lock);
	return filters.get();
}

unique_ptr<TableFilterSet> DynamicTableFilterSet::GetFinalTableFilters(optional_ptr<TableFilterSet> existing_filters) const {
	D_ASSERT(HasFilters());
	auto result = make_uniq<TableFilterSet>();
	if (existing_filters) {
		for (auto &entry : existing_filters->filters) {
			result->filters[entry.first] = entry.second->Copy();
		}
	}
	for (auto &entry : filters->filters) {
		result->filters[entry.first] = entry.second->Copy();
	}
	return result;
}

} // namespace duckdb
