#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

TableFilterSet::ConstTableFilterIteratorEntry::ConstTableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::const_iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::ConstTableFilterIteratorEntry::ColumnIndex() const {
	return iterator->first;
}

const TableFilter &TableFilterSet::ConstTableFilterIteratorEntry::Filter() const {
	return *iterator->second;
}

TableFilterSet::TableFilterIteratorEntry::TableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::TableFilterIteratorEntry::ColumnIndex() const {
	return iterator->first;
}

TableFilter &TableFilterSet::TableFilterIteratorEntry::Filter() {
	return *iterator->second;
}

const TableFilter &TableFilterSet::TableFilterIteratorEntry::Filter() const {
	return *iterator->second;
}

unique_ptr<TableFilter> TableFilterSet::TableFilterIteratorEntry::TakeFilter() {
	return std::move(iterator->second);
}

bool TableFilterSet::HasFilters() const {
	return !filters.empty();
}
idx_t TableFilterSet::FilterCount() const {
	return filters.size();
}
bool TableFilterSet::HasFilter(ProjectionIndex col_idx) const {
	return filters.find(col_idx) != filters.end();
}

const TableFilter &TableFilterSet::GetFilterByColumnIndex(ProjectionIndex col_idx) const {
	auto filter = TryGetFilterByColumnIndex(col_idx);
	if (!filter) {
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx.index);
	}
	return *filter;
}

optional_ptr<const TableFilter> TableFilterSet::TryGetFilterByColumnIndex(ProjectionIndex col_idx) const {
	if (!col_idx.IsValid()) {
		throw InternalException("TableFilterSet::TryGetFilterByColumnIndex called with invalid column index");
	}
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		return nullptr;
	}
	return *entry->second;
}

TableFilter &TableFilterSet::GetFilterByColumnIndexMutable(ProjectionIndex col_idx) {
	auto filter = TryGetFilterByColumnIndexMutable(col_idx);
	if (!filter) {
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx.index);
	}
	return *filter;
}

optional_ptr<TableFilter> TableFilterSet::TryGetFilterByColumnIndexMutable(ProjectionIndex col_idx) {
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		return nullptr;
	}
	return *entry->second;
}

void TableFilterSet::RemoveFilterByColumnIndex(ProjectionIndex col_idx) {
	filters.erase(col_idx);
}

void TableFilterSet::SetFilterByColumnIndex(ProjectionIndex col_idx, unique_ptr<TableFilter> filter) {
	filters[col_idx] = std::move(filter);
}

void TableFilterSet::ClearFilters() {
	filters.clear();
}

bool TableFilterSet::Equals(TableFilterSet &other) {
	if (filters.size() != other.filters.size()) {
		return false;
	}
	for (auto &entry : filters) {
		auto other_entry = other.filters.find(entry.first);
		if (other_entry == other.filters.end()) {
			return false;
		}
		if (!entry.second->Equals(*other_entry->second)) {
			return false;
		}
	}
	return true;
}

bool TableFilterSet::Equals(TableFilterSet *left, TableFilterSet *right) {
	if (left == right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

unique_ptr<TableFilterSet> TableFilterSet::Copy() const {
	auto copy = make_uniq<TableFilterSet>();
	for (auto &it : filters) {
		copy->filters.emplace(it.first, it.second->Copy());
	}
	return copy;
}

void TableFilterSet::PushFilter(ProjectionIndex col_idx, unique_ptr<TableFilter> filter) {
	if (!col_idx.IsValid()) {
		throw InternalException("Cannot push a filter over an invalid ProjectionIndex");
	}
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[col_idx] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = entry->second->Cast<ConjunctionAndFilter>();
			and_filter.child_filters.push_back(std::move(filter));
		} else {
			auto and_filter = make_uniq<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[col_idx] = std::move(and_filter);
		}
	}
}

void DynamicTableFilterSet::ClearFilters(const PhysicalOperator &op) {
	lock_guard<mutex> l(lock);
	filters.erase(op);
}

void DynamicTableFilterSet::PushFilter(const PhysicalOperator &op, ProjectionIndex column_index,
                                       unique_ptr<TableFilter> filter) {
	lock_guard<mutex> l(lock);
	auto entry = filters.find(op);
	optional_ptr<TableFilterSet> filter_ptr;
	if (entry == filters.end()) {
		auto filter_set = make_uniq<TableFilterSet>();
		filter_ptr = filter_set.get();
		filters[op] = std::move(filter_set);
	} else {
		filter_ptr = entry->second.get();
	}
	filter_ptr->PushFilter(column_index, std::move(filter));
}

bool DynamicTableFilterSet::HasFilters() const {
	lock_guard<mutex> l(lock);
	return !filters.empty();
}

unique_ptr<TableFilterSet>
DynamicTableFilterSet::GetFinalTableFilters(const PhysicalTableScan &scan,
                                            optional_ptr<TableFilterSet> existing_filters) const {
	lock_guard<mutex> l(lock);
	D_ASSERT(!filters.empty());
	auto result = make_uniq<TableFilterSet>();
	if (existing_filters) {
		for (auto &filter_entry : *existing_filters) {
			result->PushFilter(filter_entry.ColumnIndex(), filter_entry.Filter().Copy());
		}
	}
	for (auto &entry : filters) {
		for (auto &filter_entry : *entry.second) {
			result->PushFilter(filter_entry.ColumnIndex(), filter_entry.Filter().Copy());
		}
	}
	if (!result->HasFilters()) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
