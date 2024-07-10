#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

void TableScanState::Initialize(vector<column_t> column_ids_p, optional_ptr<TableFilterSet> table_filters) {
	this->column_ids = std::move(column_ids_p);
	if (table_filters) {
		filters.Initialize(*table_filters, column_ids);
	}
}

const vector<column_t> &TableScanState::GetColumnIds() {
	D_ASSERT(!column_ids.empty());
	return column_ids;
}

ScanFilterInfo &TableScanState::GetFilterInfo() {
	return filters;
}

ScanFilter::ScanFilter(idx_t index, const vector<column_t> &column_ids, TableFilter &filter) :
	scan_column_index(index), table_column_index(column_ids[index]), filter(filter) {
}

void ScanFilterInfo::Initialize(TableFilterSet &filters, const vector<column_t> &column_ids) {
	D_ASSERT(!filters.filters.empty());
	table_filters = &filters;
	adaptive_filter = make_uniq<AdaptiveFilter>(filters);
	filter_list.reserve(filters.filters.size());
	for(auto &entry : filters.filters) {
		filter_list.emplace_back(entry.first, column_ids, *entry.second);
	}
}

bool ScanFilterInfo::ColumnHasFilters(idx_t column_idx) {
	if (!table_filters) {
		return false;
	}
	if (table_filters->filters.find(column_idx) != table_filters->filters.end()) {
		return true;
	}
	return false;
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

	TableFilterSet &GetFilters();

ScanFilterInfo &CollectionScanState::GetFilterInfo() {
	return parent.GetFilterInfo();
}

TableScanOptions &CollectionScanState::GetOptions() {
	return parent.options;
}

ParallelCollectionScanState::ParallelCollectionScanState()
    : collection(nullptr), current_row_group(nullptr), processed_rows(0) {
}

CollectionScanState::CollectionScanState(TableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      valid_sel(STANDARD_VECTOR_SIZE), parent(parent_p) {
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

PrefetchState::~PrefetchState() {
}

void PrefetchState::AddBlock(shared_ptr<BlockHandle> block) {
	blocks.push_back(std::move(block));
}

} // namespace duckdb
