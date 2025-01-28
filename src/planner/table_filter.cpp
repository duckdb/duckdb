#include "duckdb/planner/table_filter.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

void TableFilterSet::PushFilter(const ColumnIndex &col_idx, unique_ptr<TableFilter> filter) {
	auto column_index = col_idx.GetPrimaryIndex();
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

string TableFilter::DebugToString() {
	return ToString("c0");
}

void DynamicTableFilterSet::ClearFilters(const PhysicalOperator &op) {
	lock_guard<mutex> l(lock);
	filters.erase(op);
}

void DynamicTableFilterSet::PushFilter(const PhysicalOperator &op, idx_t column_index, unique_ptr<TableFilter> filter) {
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
	filter_ptr->PushFilter(ColumnIndex(column_index), std::move(filter));
}

void DynamicTableFilterSet::PushBloomFilter(const PhysicalOperator &op, unique_ptr<JoinBloomFilter> bloom_filter) {
	lock_guard<mutex> l(lock);
	auto entry = bloom_filters.find(op);
	if (entry == bloom_filters.end()) {
		bloom_filters[op] = {};
	}
	bloom_filters[op].push_back(std::move(bloom_filter));
}

JoinBloomFilter *DynamicTableFilterSet::GetPtrToLastBf(const PhysicalOperator &op) {
	lock_guard<mutex> l(lock);
	const auto& entry = bloom_filters.find(op);
	return entry->second.back().get();
}

bool DynamicTableFilterSet::HasFilters() const {
	lock_guard<mutex> l(lock);
	return !filters.empty();
}

bool DynamicTableFilterSet::HasBloomFilters() const {
	lock_guard<mutex> l(lock);
	return !bloom_filters.empty();
}

unique_ptr<TableFilterSet>
DynamicTableFilterSet::GetFinalTableFilters(const PhysicalTableScan &scan,
                                            optional_ptr<TableFilterSet> existing_filters) const {
	D_ASSERT(HasFilters());
	auto result = make_uniq<TableFilterSet>();
	if (existing_filters) {
		for (auto &entry : existing_filters->filters) {
			result->PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}
	for (auto &entry : filters) {
		for (auto &filter : entry.second->filters) {
			result->PushFilter(ColumnIndex(filter.first), filter.second->Copy());
		}
	}
	if (result->filters.empty()) {
		return nullptr;
	}
	return result;
}

unique_ptr<vector<unique_ptr<JoinBloomFilter>>> DynamicTableFilterSet::GetBloomFilters() const {
	D_ASSERT(HasBloomFilters());
	auto result = make_uniq<vector<unique_ptr<JoinBloomFilter>>>();
	for (auto &entry : bloom_filters) {
		for (auto &filter : entry.second) {
			result->push_back(make_uniq<JoinBloomFilter>(filter->Copy()));
		}
	}
	return result;
}

} // namespace duckdb
