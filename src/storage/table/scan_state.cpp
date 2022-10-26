#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

void TableScanState::Initialize(vector<column_t> column_ids, TableFilterSet *table_filters) {
	this->column_ids = move(column_ids);
	this->table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(table_filters->filters.size() > 0);
		this->adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
}

const vector<column_t> &TableScanState::GetColumnIds() {
	D_ASSERT(!column_ids.empty());
	return column_ids;
}

TableFilterSet *TableScanState::GetFilters() {
	D_ASSERT(!table_filters || adaptive_filter.get());
	return table_filters;
}

AdaptiveFilter *TableScanState::GetAdaptiveFilter() {
	return adaptive_filter.get();
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->start + current->count) {
		current = (ColumnSegment *)current->Next();
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (row_index >= current->start && row_index < current->start + current->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

void ColumnScanState::NextVector() {
	Next(STANDARD_VECTOR_SIZE);
}

const vector<column_t> &RowGroupScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet *RowGroupScanState::GetFilters() {
	return parent.GetFilters();
}

AdaptiveFilter *RowGroupScanState::GetAdaptiveFilter() {
	return parent.GetAdaptiveFilter();
}

idx_t RowGroupScanState::GetParentMaxRow() {
	return parent.max_row;
}

const vector<column_t> &CollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet *CollectionScanState::GetFilters() {
	return parent.GetFilters();
}

AdaptiveFilter *CollectionScanState::GetAdaptiveFilter() {
	return parent.GetAdaptiveFilter();
}

bool CollectionScanState::Scan(Transaction &transaction, DataChunk &result) {
	auto current_row_group = row_group_state.row_group;
	while (current_row_group) {
		current_row_group->Scan(transaction, row_group_state, result);
		if (result.size() > 0) {
			return true;
		} else {
			do {
				current_row_group = row_group_state.row_group = (RowGroup *)current_row_group->Next();
				if (current_row_group) {
					bool scan_row_group = current_row_group->InitializeScan(row_group_state);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (current_row_group);
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	auto current_row_group = row_group_state.row_group;
	while (current_row_group) {
		current_row_group->ScanCommitted(row_group_state, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			current_row_group = row_group_state.row_group = (RowGroup *)current_row_group->Next();
			if (current_row_group) {
				current_row_group->InitializeScan(row_group_state);
			}
		}
	}
	return false;
}

} // namespace duckdb
