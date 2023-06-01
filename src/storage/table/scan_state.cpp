#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

void TableScanState::Initialize(vector<column_t> column_ids, TableFilterSet *table_filters) {
	this->column_ids = std::move(column_ids);
	this->table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(table_filters->filters.size() > 0);
		this->adaptive_filter = make_uniq<AdaptiveFilter>(table_filters);
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
		current = segment_tree->GetNextSegment(current);
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

const vector<storage_t> &CollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet *CollectionScanState::GetFilters() {
	return parent.GetFilters();
}

AdaptiveFilter *CollectionScanState::GetAdaptiveFilter() {
	return parent.GetAdaptiveFilter();
}

ParallelCollectionScanState::ParallelCollectionScanState()
    : collection(nullptr), current_row_group(nullptr), processed_rows(0) {
}

CollectionScanState::CollectionScanState(TableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      parent(parent_p) {
}

bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	while (row_group) {
		row_group->Scan(transaction, *this, result);
		if (result.size() > 0) {
			return true;
		} else if (max_row <= row_group->start + row_group->count) {
			row_group = nullptr;
			return false;
		} else {
			do {
				row_group = row_groups->GetNextSegment(row_group);
				if (row_group) {
					if (row_group->start >= max_row) {
						row_group = nullptr;
						break;
					}
					bool scan_row_group = row_group->InitializeScan(*this);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (row_group);
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(l, row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

} // namespace duckdb
