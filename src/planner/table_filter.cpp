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

void TableFilterSet::UnifyFilters() {
	for (auto &pair : filters) {
		if (pair.second->filter_type != TableFilterType::CONJUNCTION_AND) {
			continue;
		}

		auto &and_filter = pair.second->Cast<ConjunctionAndFilter>();
		auto unified_filter = make_uniq<ConjunctionAndFilter>();

		ConstantFilter *best_greater_filter = nullptr;
		ConstantFilter *best_less_filter = nullptr;
		ConstantFilter *equal_filter = nullptr;
		for (auto &child_filter : and_filter.child_filters) {
			if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
				unified_filter->child_filters.push_back(child_filter->Copy());
				continue;
			}

			auto &constant_filter = child_filter->Cast<ConstantFilter>();
			if (constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				equal_filter = &constant_filter;
				break;
			}

			if (constant_filter.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				if (best_greater_filter == nullptr || best_greater_filter->Compare(constant_filter.constant)) {
					best_greater_filter = &constant_filter;
				}
			} else if (constant_filter.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				if (best_less_filter == nullptr || best_less_filter->Compare(constant_filter.constant)) {
					best_less_filter = &constant_filter;
				}
			} else {
				unified_filter->child_filters.push_back(child_filter->Copy());
			}
		}
		if (equal_filter) {
			pair.second = equal_filter->Copy();
			continue;
		}

		if (best_greater_filter) {
			unified_filter->child_filters.push_back(best_greater_filter->Copy());
		}
		if (best_less_filter) {
			unified_filter->child_filters.push_back(best_less_filter->Copy());
		}
		pair.second = std::move(unified_filter);
	}
}

string TableFilter::DebugToString() const {
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

bool DynamicTableFilterSet::HasFilters() const {
	lock_guard<mutex> l(lock);
	return !filters.empty();
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
			if (filter.second->filter_type == TableFilterType::CONJUNCTION_AND) {
				auto &and_filter = filter.second->Cast<ConjunctionAndFilter>();
				for (auto &child_filter : and_filter.child_filters) {
					result->PushFilter(ColumnIndex(filter.first), child_filter->Copy());
				}
			} else {
				result->PushFilter(ColumnIndex(filter.first), filter.second->Copy());
			}
		}
	}
	if (result->filters.empty()) {
		return nullptr;
	}
	result->UnifyFilters();
	return result;
}

} // namespace duckdb
