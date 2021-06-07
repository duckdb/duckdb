#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = (ConjunctionAndFilter &)*entry->second;
			and_filter.child_filters.push_back(move(filter));
		} else {
			auto and_filter = make_unique<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(move(entry->second));
			and_filter->child_filters.push_back(move(filter));
			filters[column_index] = move(and_filter);
		}
	}
}

} // namespace duckdb
