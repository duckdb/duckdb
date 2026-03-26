#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

namespace {

static void VerifyExpressionFilter(const TableFilter &filter, const char *context) {
	D_ASSERT(filter.filter_type == TableFilterType::EXPRESSION_FILTER);
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw InternalException("%s expected ExpressionFilter, got %s", context,
		                        EnumUtil::ToString(filter.filter_type));
	}
}

} // namespace

TableFilterSet::ConstTableFilterIteratorEntry::ConstTableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::const_iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::ConstTableFilterIteratorEntry::GetIndex() const {
	return iterator->first;
}

const TableFilter &TableFilterSet::ConstTableFilterIteratorEntry::Filter() const {
	return *iterator->second;
}

TableFilterSet::TableFilterIteratorEntry::TableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::TableFilterIteratorEntry::GetIndex() const {
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
	return !filters.empty() || !generic_filters.empty();
}
bool TableFilterSet::HasColumnFilters() const {
	return !filters.empty();
}
bool TableFilterSet::HasGenericFilters() const {
	return !generic_filters.empty();
}
idx_t TableFilterSet::FilterCount() const {
	return filters.size();
}
idx_t TableFilterSet::GenericFilterCount() const {
	return generic_filters.size();
}
bool TableFilterSet::HasFilter(ProjectionIndex col_idx) const {
	return filters.find(col_idx) != filters.end();
}

const TableFilter &TableFilterSet::GetFilterByColumnIndex(ProjectionIndex col_idx) const {
	auto filter = TryGetFilterByColumnIndex(col_idx);
	if (!filter) {
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx);
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
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx);
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
	VerifyExpressionFilter(*filter, "TableFilterSet::SetFilterByColumnIndex");
	filters[col_idx] = std::move(filter);
}

void TableFilterSet::ClearFilters() {
	filters.clear();
	generic_filters.clear();
}

bool TableFilterSet::Equals(TableFilterSet &other) {
	if (filters.size() != other.filters.size()) {
		return false;
	}
	if (generic_filters.size() != other.generic_filters.size()) {
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
	for (idx_t i = 0; i < generic_filters.size(); i++) {
		if (!generic_filters[i]->Equals(*other.generic_filters[i])) {
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
	for (auto &filter : generic_filters) {
		copy->generic_filters.push_back(filter->Copy());
	}
	return copy;
}

void TableFilterSet::PushFilter(unique_ptr<Expression> filter) {
	generic_filters.push_back(std::move(filter));
}

void TableFilterSet::PushFilter(ProjectionIndex col_idx, unique_ptr<TableFilter> filter) {
	if (!col_idx.IsValid()) {
		throw InternalException("Cannot push a filter over an invalid ProjectionIndex");
	}
	VerifyExpressionFilter(*filter, "TableFilterSet::PushFilter");
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[col_idx] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		VerifyExpressionFilter(*entry->second, "TableFilterSet::PushFilter");
		auto &existing = entry->second->Cast<ExpressionFilter>();
		auto &new_filter = filter->Cast<ExpressionFilter>();
		auto and_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		and_expr->children.push_back(std::move(existing.expr));
		and_expr->children.push_back(std::move(new_filter.expr));
		filters[col_idx] = make_uniq<ExpressionFilter>(std::move(and_expr));
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
			result->PushFilter(filter_entry.GetIndex(), filter_entry.Filter().Copy());
		}
		for (auto &filter : existing_filters->GetGenericFilters()) {
			result->PushFilter(filter->Copy());
		}
	}
	for (auto &entry : filters) {
		for (auto &filter_entry : *entry.second) {
			result->PushFilter(filter_entry.GetIndex(), filter_entry.Filter().Copy());
		}
	}
	if (!result->HasFilters()) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
