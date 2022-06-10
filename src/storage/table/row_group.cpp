#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

constexpr const idx_t RowGroup::ROW_GROUP_VECTOR_COUNT;
constexpr const idx_t RowGroup::ROW_GROUP_SIZE;

RowGroup::RowGroup(DatabaseInstance &db, DataTableInfo &table_info, idx_t start, idx_t count)
    : SegmentBase(start, count), db(db), table_info(table_info) {

	Verify();
}

RowGroup::RowGroup(DatabaseInstance &db, DataTableInfo &table_info, const vector<LogicalType> &types,
                   RowGroupPointer &pointer)
    : SegmentBase(pointer.row_start, pointer.tuple_count), db(db), table_info(table_info) {
	// deserialize the columns
	if (pointer.data_pointers.size() != types.size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	for (idx_t i = 0; i < pointer.data_pointers.size(); i++) {
		auto &block_pointer = pointer.data_pointers[i];
		MetaBlockReader column_data_reader(db, block_pointer.block_id);
		column_data_reader.offset = block_pointer.offset;
		this->columns.push_back(ColumnData::Deserialize(table_info, i, start, column_data_reader, types[i], nullptr));
	}

	// set up the statistics
	for (auto &stats : pointer.statistics) {
		auto stats_type = stats->type;
		this->stats.push_back(make_shared<SegmentStatistics>(stats_type, move(stats)));
	}
	this->version_info = move(pointer.versions);

	Verify();
}

RowGroup::~RowGroup() {
}

void RowGroup::InitializeEmpty(const vector<LogicalType> &types) {
	// set up the segment trees for the column segments
	for (idx_t i = 0; i < types.size(); i++) {
		auto column_data = ColumnData::CreateColumn(GetTableInfo(), i, start, types[i]);
		stats.push_back(make_shared<SegmentStatistics>(types[i]));
		columns.push_back(move(column_data));
	}
}

bool RowGroup::InitializeScanWithOffset(RowGroupScanState &state, idx_t vector_offset) {
	auto &column_ids = state.parent.column_ids;
	if (state.parent.table_filters) {
		if (!CheckZonemap(*state.parent.table_filters, column_ids)) {
			return false;
		}
	}

	state.row_group = this;
	state.vector_index = vector_offset;
	state.max_row =
	    this->start > state.parent.max_row ? 0 : MinValue<idx_t>(this->count, state.parent.max_row - this->start);
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column]->InitializeScanWithOffset(state.column_scans[i],
			                                          start + vector_offset * STANDARD_VECTOR_SIZE);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

bool RowGroup::InitializeScan(RowGroupScanState &state) {
	auto &column_ids = state.parent.column_ids;
	if (state.parent.table_filters) {
		if (!CheckZonemap(*state.parent.table_filters, column_ids)) {
			return false;
		}
	}
	state.row_group = this;
	state.vector_index = 0;
	state.max_row =
	    this->start > state.parent.max_row ? 0 : MinValue<idx_t>(this->count, state.parent.max_row - this->start);
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column]->InitializeScan(state.column_scans[i]);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

unique_ptr<RowGroup> RowGroup::AlterType(ClientContext &context, const LogicalType &target_type, idx_t changed_idx,
                                         ExpressionExecutor &executor, TableScanState &scan_state,
                                         DataChunk &scan_chunk) {
	Verify();

	// construct a new column data for this type
	auto column_data = ColumnData::CreateColumn(GetTableInfo(), changed_idx, start, target_type);

	ColumnAppendState append_state;
	column_data->InitializeAppend(append_state);

	// scan the original table, and fill the new column with the transformed value
	InitializeScan(scan_state.row_group_scan_state);

	Vector append_vector(target_type);
	auto altered_col_stats = make_shared<SegmentStatistics>(target_type);
	while (true) {
		// scan the table
		scan_chunk.Reset();
		ScanCommitted(scan_state.row_group_scan_state, scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (scan_chunk.size() == 0) {
			break;
		}
		// execute the expression
		executor.ExecuteExpression(scan_chunk, append_vector);
		column_data->Append(*altered_col_stats->statistics, append_state, append_vector, scan_chunk.size());
	}

	// set up the row_group based on this row_group
	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i == changed_idx) {
			// this is the altered column: use the new column
			row_group->columns.push_back(move(column_data));
			row_group->stats.push_back(move(altered_col_stats));
		} else {
			// this column was not altered: use the data directly
			row_group->columns.push_back(columns[i]);
			row_group->stats.push_back(stats[i]);
		}
	}
	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::AddColumn(ClientContext &context, ColumnDefinition &new_column,
                                         ExpressionExecutor &executor, Expression *default_value, Vector &result) {
	Verify();

	// construct a new column data for the new column
	auto added_column = ColumnData::CreateColumn(GetTableInfo(), columns.size(), start, new_column.Type());
	auto added_col_stats = make_shared<SegmentStatistics>(
	    new_column.Type(), BaseStatistics::CreateEmpty(new_column.Type(), StatisticsType::LOCAL_STATS));

	idx_t rows_to_write = this->count;
	if (rows_to_write > 0) {
		DataChunk dummy_chunk;

		ColumnAppendState state;
		added_column->InitializeAppend(state);
		for (idx_t i = 0; i < rows_to_write; i += STANDARD_VECTOR_SIZE) {
			idx_t rows_in_this_vector = MinValue<idx_t>(rows_to_write - i, STANDARD_VECTOR_SIZE);
			if (default_value) {
				dummy_chunk.SetCardinality(rows_in_this_vector);
				executor.ExecuteExpression(dummy_chunk, result);
			}
			added_column->Append(*added_col_stats->statistics, state, result, rows_in_this_vector);
		}
	}

	// set up the row_group based on this row_group
	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	row_group->columns = columns;
	row_group->stats = stats;
	// now add the new column
	row_group->columns.push_back(move(added_column));
	row_group->stats.push_back(move(added_col_stats));

	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::RemoveColumn(idx_t removed_column) {
	Verify();

	D_ASSERT(removed_column < columns.size());

	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	row_group->columns = columns;
	row_group->stats = stats;
	// now remove the column
	row_group->columns.erase(row_group->columns.begin() + removed_column);
	row_group->stats.erase(row_group->stats.begin() + removed_column);

	row_group->Verify();
	return row_group;
}

void RowGroup::CommitDrop() {
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		CommitDropColumn(column_idx);
	}
}

void RowGroup::CommitDropColumn(idx_t column_idx) {
	D_ASSERT(column_idx < columns.size());
	columns[column_idx]->CommitDropColumn();
}

void RowGroup::NextVector(RowGroupScanState &state) {
	state.vector_index++;
	for (idx_t i = 0; i < state.parent.column_ids.size(); i++) {
		auto column = state.parent.column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		D_ASSERT(column < columns.size());
		columns[column]->Skip(state.column_scans[i]);
	}
}

bool RowGroup::CheckZonemap(TableFilterSet &filters, const vector<column_t> &column_ids) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		auto base_column_index = column_ids[column_index];

		auto propagate_result = filter->CheckStatistics(*stats[base_column_index]->statistics);
		if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
		    propagate_result == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
			return false;
		}
	}
	return true;
}

bool RowGroup::CheckZonemapSegments(RowGroupScanState &state) {
	if (!state.parent.table_filters) {
		return true;
	}
	auto &column_ids = state.parent.column_ids;
	for (auto &entry : state.parent.table_filters->filters) {
		D_ASSERT(entry.first < column_ids.size());
		auto column_idx = entry.first;
		auto base_column_idx = column_ids[column_idx];
		bool read_segment = columns[base_column_idx]->CheckZonemap(state.column_scans[column_idx], *entry.second);
		if (!read_segment) {
			idx_t target_row =
			    state.column_scans[column_idx].current->start + state.column_scans[column_idx].current->count;
			D_ASSERT(target_row >= this->start);
			D_ASSERT(target_row <= this->start + this->count);
			idx_t target_vector_index = (target_row - this->start) / STANDARD_VECTOR_SIZE;
			if (state.vector_index == target_vector_index) {
				// we can't skip any full vectors because this segment contains less than a full vector
				// for now we just bail-out
				// FIXME: we could check if we can ALSO skip the next segments, in which case skipping a full vector
				// might be possible
				// we don't care that much though, since a single segment that fits less than a full vector is
				// exceedingly rare
				return true;
			}
			while (state.vector_index < target_vector_index) {
				NextVector(state);
			}
			return false;
		}
	}

	return true;
}

template <TableScanType TYPE>
void RowGroup::TemplatedScan(Transaction *transaction, RowGroupScanState &state, DataChunk &result) {
	const bool ALLOW_UPDATES = TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES &&
	                           TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	auto &table_filters = state.parent.table_filters;
	auto &column_ids = state.parent.column_ids;
	auto &adaptive_filter = state.parent.adaptive_filter;
	while (true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row - current_row);

		//! first check the zonemap if we have to scan this partition
		if (!CheckZonemapSegments(state)) {
			continue;
		}
		// second, scan the version chunk manager to figure out which tuples to load for this transaction
		idx_t count;
		SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
		if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
			D_ASSERT(transaction);
			count = state.row_group->GetSelVector(*transaction, state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else if (TYPE == TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
			auto &transaction_manager = TransactionManager::Get(db);
			auto lowest_active_start = transaction_manager.LowestActiveStart();
			auto lowest_active_id = transaction_manager.LowestActiveId();

			count = state.row_group->GetCommittedSelVector(lowest_active_start, lowest_active_id, state.vector_index,
			                                               valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else {
			count = max_count;
		}
		if (count == max_count && !table_filters) {
			// scan all vectors completely: full scan without deletions or table filters
			for (idx_t i = 0; i < column_ids.size(); i++) {
				auto column = column_ids[i];
				if (column == COLUMN_IDENTIFIER_ROW_ID) {
					// scan row id
					D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
					result.data[i].Sequence(this->start + current_row, 1);
				} else {
					if (TYPE != TableScanType::TABLE_SCAN_REGULAR) {
						columns[column]->ScanCommitted(state.vector_index, state.column_scans[i], result.data[i],
						                               ALLOW_UPDATES);
					} else {
						D_ASSERT(transaction);
						columns[column]->Scan(*transaction, state.vector_index, state.column_scans[i], result.data[i]);
					}
				}
			}
		} else {
			// partial scan: we have deletions or table filters
			idx_t approved_tuple_count = count;
			SelectionVector sel;
			if (count != max_count) {
				sel.Initialize(valid_sel);
			} else {
				sel.Initialize(nullptr);
			}
			//! first, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto start_time = high_resolution_clock::now();
			if (table_filters) {
				D_ASSERT(ALLOW_UPDATES);
				for (idx_t i = 0; i < table_filters->filters.size(); i++) {
					auto tf_idx = adaptive_filter->permutation[i];
					auto col_idx = column_ids[tf_idx];
					columns[col_idx]->Select(*transaction, state.vector_index, state.column_scans[tf_idx],
					                         result.data[tf_idx], sel, approved_tuple_count,
					                         *table_filters->filters[tf_idx]);
				}
				for (auto &table_filter : table_filters->filters) {
					result.data[table_filter.first].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				// all rows were filtered out by the table filters
				// skip this vector in all the scans that were not scanned yet
				D_ASSERT(table_filters);
				result.Reset();
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = column_ids[i];
					if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
						continue;
					}
					if (table_filters->filters.find(i) == table_filters->filters.end()) {
						columns[col_idx]->Skip(state.column_scans[i]);
					}
				}
				state.vector_index++;
				continue;
			}
			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (!table_filters || table_filters->filters.find(i) == table_filters->filters.end()) {
					auto column = column_ids[i];
					if (column == COLUMN_IDENTIFIER_ROW_ID) {
						D_ASSERT(result.data[i].GetType().InternalType() == PhysicalType::INT64);
						result.data[i].SetVectorType(VectorType::FLAT_VECTOR);
						auto result_data = (int64_t *)FlatVector::GetData(result.data[i]);
						for (size_t sel_idx = 0; sel_idx < approved_tuple_count; sel_idx++) {
							result_data[sel_idx] = this->start + current_row + sel.get_index(sel_idx);
						}
					} else {
						if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
							D_ASSERT(transaction);
							columns[column]->FilterScan(*transaction, state.vector_index, state.column_scans[i],
							                            result.data[i], sel, approved_tuple_count);
						} else {
							D_ASSERT(!transaction);
							columns[column]->FilterScanCommitted(state.vector_index, state.column_scans[i],
							                                     result.data[i], sel, approved_tuple_count,
							                                     ALLOW_UPDATES);
						}
					}
				}
			}
			auto end_time = high_resolution_clock::now();
			if (adaptive_filter && table_filters->filters.size() > 1) {
				adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
			}
			D_ASSERT(approved_tuple_count > 0);
			count = approved_tuple_count;
		}
		result.SetCardinality(count);
		state.vector_index++;
		break;
	}
}

void RowGroup::Scan(Transaction &transaction, RowGroupScanState &state, DataChunk &result) {
	TemplatedScan<TableScanType::TABLE_SCAN_REGULAR>(&transaction, state, result);
}

void RowGroup::ScanCommitted(RowGroupScanState &state, DataChunk &result, TableScanType type) {
	switch (type) {
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS>(nullptr, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES>(nullptr, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(nullptr, state, result);
		break;
	default:
		throw InternalException("Unrecognized table scan type");
	}
}

ChunkInfo *RowGroup::GetChunkInfo(idx_t vector_idx) {
	if (!version_info) {
		return nullptr;
	}
	return version_info->info[vector_idx].get();
}

idx_t RowGroup::GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetCommittedSelVector(start_time, transaction_id, sel_vector, max_count);
}

bool RowGroup::Fetch(Transaction &transaction, idx_t row) {
	D_ASSERT(row < this->count);
	lock_guard<mutex> lock(row_group_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void RowGroup::FetchRow(Transaction &transaction, ColumnFetchState &state, const vector<column_t> &column_ids,
                        row_t row_id, DataChunk &result, idx_t result_idx) {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			D_ASSERT(result.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
			result.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
			auto data = FlatVector::GetData<row_t>(result.data[col_idx]);
			data[result_idx] = row_id;
		} else {
			// regular column: fetch data from the base column
			columns[column]->FetchRow(transaction, state, row_id, result.data[col_idx], result_idx);
		}
	}
}

void RowGroup::AppendVersionInfo(Transaction &transaction, idx_t row_group_start, idx_t count,
                                 transaction_t commit_id) {
	idx_t row_group_end = row_group_start + count;
	lock_guard<mutex> lock(row_group_lock);

	this->count += count;
	D_ASSERT(this->count <= RowGroup::ROW_GROUP_SIZE);

	// create the version_info if it doesn't exist yet
	if (!version_info) {
		version_info = make_unique<VersionNode>();
	}
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (start == 0 && end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_unique<ChunkConstantInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
			constant_info->insert_id = commit_id;
			constant_info->delete_id = NOT_DELETED_ID;
			version_info->info[vector_idx] = move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			ChunkVectorInfo *info;
			if (!version_info->info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_unique<ChunkVectorInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
				info = insert_info.get();
				version_info->info[vector_idx] = move(insert_info);
			} else {
				D_ASSERT(version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				info = (ChunkVectorInfo *)version_info->info[vector_idx].get();
			}
			info->Append(start, end, commit_id);
		}
	}
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	D_ASSERT(version_info.get());
	idx_t row_group_end = row_group_start + count;
	lock_guard<mutex> lock(row_group_lock);

	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = version_info->info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void RowGroup::RevertAppend(idx_t row_group_start) {
	if (!version_info) {
		return;
	}
	idx_t start_row = row_group_start - this->start;
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		version_info->info[vector_idx].reset();
	}
	for (auto &column : columns) {
		column->RevertAppend(row_group_start);
	}
	this->count = MinValue<idx_t>(row_group_start - this->start, this->count);
	Verify();
}

void RowGroup::InitializeAppend(Transaction &transaction, RowGroupAppendState &append_state,
                                idx_t remaining_append_count) {
	append_state.row_group = this;
	append_state.offset_in_row_group = this->count;
	// for each column, initialize the append state
	append_state.states = unique_ptr<ColumnAppendState[]>(new ColumnAppendState[columns.size()]);
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->InitializeAppend(append_state.states[i]);
	}
	// append the version info for this row_group
	idx_t append_count = MinValue<idx_t>(remaining_append_count, RowGroup::ROW_GROUP_SIZE - this->count);
	AppendVersionInfo(transaction, this->count, append_count, transaction.transaction_id);
}

void RowGroup::Append(RowGroupAppendState &state, DataChunk &chunk, idx_t append_count) {
	// append to the current row_group
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->Append(*stats[i]->statistics, state.states[i], chunk.data[i], append_count);
	}
	state.offset_in_row_group += append_count;
}

void RowGroup::Update(Transaction &transaction, DataChunk &update_chunk, row_t *ids, idx_t offset, idx_t count,
                      const vector<column_t> &column_ids) {
#ifdef DEBUG
	for (size_t i = offset; i < offset + count; i++) {
		D_ASSERT(ids[i] >= row_t(this->start) && ids[i] < row_t(this->start + this->count));
	}
#endif
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		D_ASSERT(column != COLUMN_IDENTIFIER_ROW_ID);
		D_ASSERT(columns[column]->type.id() == update_chunk.data[i].GetType().id());
		if (offset > 0) {
			Vector sliced_vector(update_chunk.data[i], offset);
			sliced_vector.Normalify(count);
			columns[column]->Update(transaction, column, sliced_vector, ids + offset, count);
		} else {
			columns[column]->Update(transaction, column, update_chunk.data[i], ids, count);
		}
		MergeStatistics(column, *columns[column]->GetUpdateStatistics());
	}
}

void RowGroup::UpdateColumn(Transaction &transaction, DataChunk &updates, Vector &row_ids,
                            const vector<column_t> &column_path) {
	D_ASSERT(updates.ColumnCount() == 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);

	auto primary_column_idx = column_path[0];
	D_ASSERT(primary_column_idx != COLUMN_IDENTIFIER_ROW_ID);
	D_ASSERT(primary_column_idx < columns.size());
	columns[primary_column_idx]->UpdateColumn(transaction, column_path, updates.data[0], ids, updates.size(), 1);
	MergeStatistics(primary_column_idx, *columns[primary_column_idx]->GetUpdateStatistics());
}

unique_ptr<BaseStatistics> RowGroup::GetStatistics(idx_t column_idx) {
	D_ASSERT(column_idx < stats.size());

	lock_guard<mutex> slock(stats_lock);
	return stats[column_idx]->statistics->Copy();
}

void RowGroup::MergeStatistics(idx_t column_idx, BaseStatistics &other) {
	D_ASSERT(column_idx < stats.size());

	lock_guard<mutex> slock(stats_lock);
	stats[column_idx]->statistics->Merge(other);
}

RowGroupPointer RowGroup::Checkpoint(TableDataWriter &writer, vector<unique_ptr<BaseStatistics>> &global_stats) {
	vector<unique_ptr<ColumnCheckpointState>> states;
	states.reserve(columns.size());

	// checkpoint the individual columns of the row group
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		auto &column = columns[column_idx];
		ColumnCheckpointInfo checkpoint_info {writer.GetColumnCompressionType(column_idx)};
		auto checkpoint_state = column->Checkpoint(*this, writer, checkpoint_info);
		D_ASSERT(checkpoint_state);

		auto stats = checkpoint_state->GetStatistics();
		D_ASSERT(stats);

		global_stats[column_idx]->Merge(*stats);
		states.push_back(move(checkpoint_state));
	}

	// construct the row group pointer and write the column meta data to disk
	D_ASSERT(states.size() == columns.size());
	RowGroupPointer row_group_pointer;
	row_group_pointer.row_start = start;
	row_group_pointer.tuple_count = count;
	for (auto &state : states) {
		// get the current position of the meta data writer
		auto &meta_writer = writer.GetMetaWriter();
		auto pointer = meta_writer.GetBlockPointer();

		// store the stats and the data pointers in the row group pointers
		row_group_pointer.data_pointers.push_back(pointer);
		row_group_pointer.statistics.push_back(state->GetStatistics());

		// now flush the actual column data to disk
		state->FlushToDisk();
	}
	row_group_pointer.versions = version_info;
	Verify();
	return row_group_pointer;
}

void RowGroup::CheckpointDeletes(VersionNode *versions, Serializer &serializer) {
	if (!versions) {
		// no version information: write nothing
		serializer.Write<idx_t>(0);
		return;
	}
	// first count how many ChunkInfo's we need to deserialize
	idx_t chunk_info_count = 0;
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		chunk_info_count++;
	}
	// now serialize the actual version information
	serializer.Write<idx_t>(chunk_info_count);
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		serializer.Write<idx_t>(vector_idx);
		chunk_info->Serialize(serializer);
	}
}

shared_ptr<VersionNode> RowGroup::DeserializeDeletes(Deserializer &source) {
	auto chunk_count = source.Read<idx_t>();
	if (chunk_count == 0) {
		// no deletes
		return nullptr;
	}
	auto version_info = make_shared<VersionNode>();
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= RowGroup::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->info[vector_index] = ChunkInfo::Deserialize(source);
	}
	return version_info;
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &main_serializer) {
	FieldWriter writer(main_serializer);
	writer.WriteField<uint64_t>(pointer.row_start);
	writer.WriteField<uint64_t>(pointer.tuple_count);
	auto &serializer = writer.GetSerializer();
	for (auto &stats : pointer.statistics) {
		stats->Serialize(serializer);
	}
	for (auto &data_pointer : pointer.data_pointers) {
		serializer.Write<block_id_t>(data_pointer.block_id);
		serializer.Write<uint64_t>(data_pointer.offset);
	}
	CheckpointDeletes(pointer.versions.get(), serializer);
	writer.Finalize();
}

RowGroupPointer RowGroup::Deserialize(Deserializer &main_source, const vector<ColumnDefinition> &columns) {
	RowGroupPointer result;

	FieldReader reader(main_source);
	result.row_start = reader.ReadRequired<uint64_t>();
	result.tuple_count = reader.ReadRequired<uint64_t>();

	result.data_pointers.reserve(columns.size());
	result.statistics.reserve(columns.size());

	auto &source = reader.GetSource();
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (col.Generated()) {
			continue;
		}
		auto stats = BaseStatistics::Deserialize(source, columns[i].Type());
		result.statistics.push_back(move(stats));
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (col.Generated()) {
			continue;
		}
		BlockPointer pointer;
		pointer.block_id = source.Read<block_id_t>();
		pointer.offset = source.Read<uint64_t>();
		result.data_pointers.push_back(pointer);
	}
	result.versions = DeserializeDeletes(source);

	reader.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
void RowGroup::GetStorageInfo(idx_t row_group_index, vector<vector<Value>> &result) {
	for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		columns[col_idx]->GetStorageInfo(row_group_index, {col_idx}, result);
	}
}

//===--------------------------------------------------------------------===//
// Version Delete Information
//===--------------------------------------------------------------------===//
class VersionDeleteState {
public:
	VersionDeleteState(RowGroup &info, Transaction &transaction, DataTable *table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_info(nullptr),
	      current_chunk(DConstants::INVALID_INDEX), count(0), base_row(base_row), delete_count(0) {
	}

	RowGroup &info;
	Transaction &transaction;
	DataTable *table;
	ChunkVectorInfo *current_info;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;
	idx_t delete_count;

public:
	void Delete(row_t row_id);
	void Flush();
};

idx_t RowGroup::Delete(Transaction &transaction, DataTable *table, row_t *ids, idx_t count) {
	lock_guard<mutex> lock(row_group_lock);
	VersionDeleteState del_state(*this, transaction, table, this->start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= this->start && idx_t(ids[i]) < this->start + this->count);
		del_state.Delete(ids[i] - this->start);
	}
	del_state.Flush();
	return del_state.delete_count;
}

void RowGroup::Verify() {
#ifdef DEBUG
	for (auto &column : columns) {
		column->Verify(*this);
	}
#endif
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		if (!info.version_info) {
			info.version_info = make_unique<VersionNode>();
		}

		if (!info.version_info->info[vector_idx]) {
			// no info yet: create it
			info.version_info->info[vector_idx] =
			    make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
		} else if (info.version_info->info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
			auto &constant = (ChunkConstantInfo &)*info.version_info->info[vector_idx];
			// info exists but it's a constant info: convert to a vector info
			auto new_info = make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
			new_info->insert_id = constant.insert_id.load();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				new_info->inserted[i] = constant.insert_id.load();
			}
			info.version_info->info[vector_idx] = move(new_info);
		}
		D_ASSERT(info.version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
		current_info = (ChunkVectorInfo *)info.version_info->info[vector_idx].get();
		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// delete in the current info
	delete_count += current_info->Delete(transaction, rows, count);
	// now push the delete into the undo buffer
	transaction.PushDelete(table, current_info, rows, count, base_row + chunk_row);
	count = 0;
}

} // namespace duckdb
