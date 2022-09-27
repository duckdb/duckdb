#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"

namespace duckdb {

RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, vector<LogicalType> types_p, idx_t row_start_p,
                                       idx_t total_rows_p)
    : total_rows(total_rows_p), info(move(info_p)), types(move(types_p)), row_start(row_start_p) {
	row_groups = make_shared<SegmentTree>();
}

idx_t RowGroupCollection::GetTotalRows() const {
	return total_rows.load();
}

const vector<LogicalType> &RowGroupCollection::GetTypes() const {
	return types;
}

Allocator &RowGroupCollection::GetAllocator() const {
	return Allocator::Get(info->db);
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
void RowGroupCollection::Initialize(PersistentTableData &data) {
	D_ASSERT(this->row_start == 0);
	for (auto &row_group_pointer : data.row_groups) {
		auto new_row_group = make_unique<RowGroup>(info->db, *info, types, row_group_pointer);
		auto row_group_count = new_row_group->start + new_row_group->count;
		if (row_group_count > this->total_rows) {
			this->total_rows = row_group_count;
		}
		row_groups->AppendSegment(move(new_row_group));
	}
}

void RowGroupCollection::InitializeEmpty() {
	AppendRowGroup(row_start);
}

void RowGroupCollection::AppendRowGroup(idx_t start_row) {
	D_ASSERT(start_row >= row_start);
	auto new_row_group = make_unique<RowGroup>(info->db, *info, start_row, 0);
	new_row_group->InitializeEmpty(types);
	row_groups->AppendSegment(move(new_row_group));
}

void RowGroupCollection::Verify() {
#ifdef DEBUG
	D_ASSERT(row_groups->GetRootSegment() != nullptr);
#endif
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void RowGroupCollection::InitializeScan(CollectionScanState &state, const vector<column_t> &column_ids,
                                        TableFilterSet *table_filters) {
	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	state.max_row = row_start + total_rows;
	while (row_group && !row_group->InitializeScan(state.row_group_state)) {
		row_group = (RowGroup *)row_group->next.get();
	}
}

void RowGroupCollection::InitializeCreateIndexScan(CreateIndexScanState &state) {
	state.delete_lock = std::unique_lock<mutex>(row_groups->node_lock);
}

void RowGroupCollection::InitializeScanWithOffset(CollectionScanState &state, const vector<column_t> &column_ids,
                                                  idx_t start_row, idx_t end_row) {
	auto row_group = (RowGroup *)row_groups->GetSegment(start_row);
	state.max_row = end_row;
	idx_t start_vector = (start_row - row_group->start) / STANDARD_VECTOR_SIZE;
	if (!row_group->InitializeScanWithOffset(state.row_group_state, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool RowGroupCollection::InitializeScanInRowGroup(CollectionScanState &state, RowGroup *row_group, idx_t vector_index,
                                                  idx_t max_row) {
	state.max_row = max_row;
	return row_group->InitializeScanWithOffset(state.row_group_state, vector_index);
}

void RowGroupCollection::InitializeParallelScan(ParallelCollectionScanState &state) {
	state.current_row_group = (RowGroup *)row_groups->GetRootSegment();
	state.vector_index = 0;
	state.max_row = row_start + total_rows;
}

bool RowGroupCollection::NextParallelScan(ClientContext &context, ParallelCollectionScanState &state,
                                          CollectionScanState &scan_state) {
	while (state.current_row_group && state.current_row_group->count > 0) {
		idx_t vector_index;
		idx_t max_row;
		if (ClientConfig::GetConfig(context).verify_parallelism) {
			vector_index = state.vector_index;
			max_row = state.current_row_group->start +
			          MinValue<idx_t>(state.current_row_group->count,
			                          STANDARD_VECTOR_SIZE * state.vector_index + STANDARD_VECTOR_SIZE);
			D_ASSERT(vector_index * STANDARD_VECTOR_SIZE < state.current_row_group->count);
		} else {
			vector_index = 0;
			max_row = state.current_row_group->start + state.current_row_group->count;
		}
		max_row = MinValue<idx_t>(max_row, state.max_row);
		bool need_to_scan = InitializeScanInRowGroup(scan_state, state.current_row_group, vector_index, max_row);
		if (ClientConfig::GetConfig(context).verify_parallelism) {
			state.vector_index++;
			if (state.vector_index * STANDARD_VECTOR_SIZE >= state.current_row_group->count) {
				state.current_row_group = (RowGroup *)state.current_row_group->next.get();
				state.vector_index = 0;
			}
		} else {
			state.current_row_group = (RowGroup *)state.current_row_group->next.get();
		}
		if (!need_to_scan) {
			// filters allow us to skip this row group: move to the next row group
			continue;
		}
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RowGroupCollection::Fetch(TransactionData transaction, DataChunk &result, const vector<column_t> &column_ids,
                               Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	// figure out which row_group to fetch from
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	idx_t count = 0;
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		auto row_group = (RowGroup *)row_groups->GetSegment(row_id);
		if (!row_group->Fetch(transaction, row_id - row_group->start)) {
			continue;
		}
		row_group->FetchRow(transaction, state, column_ids, row_id, result, count);
		count++;
	}
	result.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
void RowGroupCollection::InitializeAppend(TransactionData transaction, TableAppendState &state, idx_t append_count) {
	state.remaining_append_count = append_count;
	state.row_start = total_rows;
	state.current_row = state.row_start;

	// start writing to the row_groups
	lock_guard<mutex> row_group_lock(row_groups->node_lock);
	auto last_row_group = (RowGroup *)row_groups->GetLastSegment();
	D_ASSERT(this->row_start + total_rows == last_row_group->start + last_row_group->count);
	last_row_group->InitializeAppend(transaction, state.row_group_append_state, state.remaining_append_count);
	total_rows += append_count;
}

void RowGroupCollection::Append(TransactionData transaction, DataChunk &chunk, TableAppendState &state,
                                TableStatistics &stats) {
	D_ASSERT(chunk.ColumnCount() == types.size());
	chunk.Verify();

	idx_t append_count = chunk.size();
	idx_t remaining = chunk.size();
	while (true) {
		auto current_row_group = state.row_group_append_state.row_group;
		// check how much we can fit into the current row_group
		idx_t append_count =
		    MinValue<idx_t>(remaining, RowGroup::ROW_GROUP_SIZE - state.row_group_append_state.offset_in_row_group);
		if (append_count > 0) {
			current_row_group->Append(state.row_group_append_state, chunk, append_count);
			// merge the stats
			auto stats_lock = stats.GetLock();
			for (idx_t i = 0; i < types.size(); i++) {
				current_row_group->MergeIntoStatistics(i, *stats.GetStats(i).stats);
			}
		}
		state.remaining_append_count -= append_count;
		remaining -= append_count;
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
			// row_group)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				SelectionVector sel(remaining);
				for (idx_t i = 0; i < remaining; i++) {
					sel.set_index(i, append_count + i);
				}
				chunk.Slice(sel, remaining);
			}
			// append a new row_group
			AppendRowGroup(current_row_group->start + current_row_group->count);
			// set up the append state for this row_group
			lock_guard<mutex> row_group_lock(row_groups->node_lock);
			auto last_row_group = (RowGroup *)row_groups->GetLastSegment();
			last_row_group->InitializeAppend(transaction, state.row_group_append_state, state.remaining_append_count);
			continue;
		} else {
			break;
		}
	}
	state.current_row += append_count;
	auto stats_lock = stats.GetLock();
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto type = types[col_idx].InternalType();
		if (type == PhysicalType::LIST || type == PhysicalType::STRUCT) {
			continue;
		}
		stats.GetStats(col_idx).stats->UpdateDistinctStatistics(chunk.data[col_idx], chunk.size());
	}
}

void RowGroupCollection::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	auto row_group = (RowGroup *)row_groups->GetSegment(row_start);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_row_group = current_row - row_group->start;
		idx_t append_count = MinValue<idx_t>(row_group->count - start_in_row_group, remaining);

		row_group->CommitAppend(commit_id, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = (RowGroup *)row_group->next.get();
	}
}

void RowGroupCollection::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (total_rows != start_row + count) {
		// interleaved append: don't do anything
		// in this case the rows will stay as "inserted by transaction X", but will never be committed
		// they will never be used by any other transaction and will essentially leave a gap
		// this situation is rare, and as such we don't care about optimizing it (yet?)
		// it only happens if C1 appends a lot of data -> C2 appends a lot of data -> C1 rolls back
		return;
	}
	total_rows = start_row;

	lock_guard<mutex> tree_lock(row_groups->node_lock);
	// find the segment index that the current row belongs to
	idx_t segment_index = row_groups->GetSegmentIndex(start_row);
	auto segment = row_groups->nodes[segment_index].node;
	auto &info = (RowGroup &)*segment;

	// remove any segments AFTER this segment: they should be deleted entirely
	if (segment_index < row_groups->nodes.size() - 1) {
		row_groups->nodes.erase(row_groups->nodes.begin() + segment_index + 1, row_groups->nodes.end());
	}
	info.next = nullptr;
	info.RevertAppend(start_row);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
idx_t RowGroupCollection::Delete(TransactionData transaction, DataTable *table, row_t *ids, idx_t count) {
	idx_t delete_count = 0;
	// delete is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = (RowGroup *)row_groups->GetSegment(ids[pos]);
		for (pos++; pos < count; pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this row group
			if (idx_t(ids[pos]) < row_group->start) {
				// id is before row_group start -> it does not
				break;
			}
			if (idx_t(ids[pos]) >= row_group->start + row_group->count) {
				// id is after row group end -> it does not
				break;
			}
		}
		delete_count += row_group->Delete(transaction, table, ids + start, pos - start);
	} while (pos < count);
	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void RowGroupCollection::Update(TransactionData transaction, row_t *ids, const vector<column_t> &column_ids,
                                DataChunk &updates, TableStatistics &stats) {
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = (RowGroup *)row_groups->GetSegment(ids[pos]);
		row_t base_id =
		    row_group->start + ((ids[pos] - row_group->start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
		for (pos++; pos < updates.size(); pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this vector
			if (ids[pos] < base_id) {
				// id is before vector start -> it does not
				break;
			}
			if (ids[pos] >= base_id + STANDARD_VECTOR_SIZE) {
				// id is after vector end -> it does not
				break;
			}
		}
		row_group->Update(transaction, updates, ids, start, pos - start, column_ids);

		auto l = stats.GetLock();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			stats.MergeStats(*l, column_id, *row_group->GetStatistics(column_id));
		}
	} while (pos < updates.size());
}

void RowGroupCollection::RemoveFromIndexes(TableIndexList &indexes, Vector &row_identifiers, idx_t count) {
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// figure out which row_group to fetch from
	auto row_group = (RowGroup *)row_groups->GetSegment(row_ids[0]);
	auto row_group_vector_idx = (row_ids[0] - row_group->start) / STANDARD_VECTOR_SIZE;
	auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_group->start;

	// create a selection vector from the row_ids
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto row_in_vector = row_ids[i] - base_row_id;
		D_ASSERT(row_in_vector < STANDARD_VECTOR_SIZE);
		sel.set_index(i, row_in_vector);
	}

	// now fetch the columns from that row_group
	TableScanState state;
	state.table_state.max_row = row_start + total_rows;

	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	state.Initialize(move(column_ids));

	DataChunk result;
	result.Initialize(GetAllocator(), types);

	row_group->InitializeScanWithOffset(state.table_state.row_group_state, row_group_vector_idx);
	row_group->ScanCommitted(state.table_state.row_group_state, result, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
	result.Slice(sel, count);

	indexes.Scan([&](Index &index) {
		index.Delete(result, row_identifiers);
		return false;
	});
}

void RowGroupCollection::UpdateColumn(TransactionData transaction, Vector &row_ids, const vector<column_t> &column_path,
                                      DataChunk &updates, TableStatistics &stats) {
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		throw NotImplementedException("Cannot update a column-path on transaction local data");
	}
	// find the row_group this id belongs to
	auto primary_column_idx = column_path[0];
	auto row_group = (RowGroup *)row_groups->GetSegment(first_id);
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	row_group->MergeIntoStatistics(primary_column_idx, *stats.GetStats(primary_column_idx).stats);
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void RowGroupCollection::Checkpoint(TableDataWriter &writer, vector<RowGroupPointer> &row_group_pointers,
                                    vector<unique_ptr<BaseStatistics>> &global_stats) {
	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	while (row_group) {
		auto pointer = row_group->Checkpoint(writer, global_stats);
		row_group_pointers.push_back(move(pointer));
		row_group = (RowGroup *)row_group->next.get();
	}
}

//===--------------------------------------------------------------------===//
// CommitDrop
//===--------------------------------------------------------------------===//
void RowGroupCollection::CommitDropColumn(idx_t index) {
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDropColumn(index);
		segment = (RowGroup *)segment->next.get();
	}
}

void RowGroupCollection::CommitDropTable() {
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDrop();
		segment = (RowGroup *)segment->next.get();
	}
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
vector<vector<Value>> RowGroupCollection::GetStorageInfo() {
	vector<vector<Value>> result;

	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	idx_t row_group_index = 0;
	while (row_group) {
		row_group->GetStorageInfo(row_group_index, result);
		row_group_index++;

		row_group = (RowGroup *)row_group->next.get();
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Alter
//===--------------------------------------------------------------------===//
shared_ptr<RowGroupCollection> RowGroupCollection::AddColumn(ColumnDefinition &new_column, Expression *default_value,
                                                             ColumnStatistics &stats) {
	idx_t new_column_idx = types.size();
	auto new_types = types;
	new_types.push_back(new_column.GetType());
	auto result = make_shared<RowGroupCollection>(info, move(new_types), row_start, total_rows.load());

	ExpressionExecutor executor(GetAllocator());
	DataChunk dummy_chunk;
	Vector default_vector(new_column.GetType());
	if (!default_value) {
		FlatVector::Validity(default_vector).SetAllInvalid(STANDARD_VECTOR_SIZE);
	} else {
		executor.AddExpression(*default_value);
	}

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_unique<SegmentStatistics>(new_column.GetType());
	auto current_row_group = (RowGroup *)row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->AddColumn(new_column, executor, default_value, default_vector);
		// merge in the statistics
		new_row_group->MergeIntoStatistics(new_column_idx, *stats.stats);

		result->row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::RemoveColumn(idx_t col_idx) {
	D_ASSERT(col_idx < types.size());
	auto new_types = types;
	new_types.erase(new_types.begin() + col_idx);

	auto result = make_shared<RowGroupCollection>(info, move(new_types), row_start, total_rows.load());

	auto current_row_group = (RowGroup *)row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->RemoveColumn(col_idx);
		result->row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::AlterType(idx_t changed_idx, const LogicalType &target_type,
                                                             vector<column_t> bound_columns, Expression &cast_expr,
                                                             ColumnStatistics &stats) {
	D_ASSERT(changed_idx < types.size());
	auto new_types = types;
	new_types[changed_idx] = target_type;

	auto result = make_shared<RowGroupCollection>(info, move(new_types), row_start, total_rows.load());

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(types[bound_columns[i]]);
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	ExpressionExecutor executor(GetAllocator());
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.Initialize(bound_columns);
	scan_state.table_state.max_row = row_start + total_rows;

	// now alter the type of the column within all of the row_groups individually
	auto current_row_group = (RowGroup *)row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->AlterType(target_type, changed_idx, executor,
		                                                  scan_state.table_state.row_group_state, scan_chunk);
		new_row_group->MergeIntoStatistics(changed_idx, *stats.stats);
		result->row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	return result;
}

void RowGroupCollection::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	// scan the original table, check if there's any null value
	auto &not_null_constraint = (BoundNotNullConstraint &)constraint;
	vector<LogicalType> scan_types;
	D_ASSERT(not_null_constraint.index < types.size());
	scan_types.push_back(types[not_null_constraint.index]);
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	CreateIndexScanState state;
	vector<column_t> cids;
	cids.push_back(not_null_constraint.index);
	// Use ScanCommitted to scan the latest committed data
	InitializeCreateIndexScan(state);
	state.Initialize(cids, nullptr);
	InitializeScan(state.table_state, cids, nullptr);
	while (true) {
		scan_chunk.Reset();
		state.table_state.ScanCommitted(scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		if (scan_chunk.size() == 0) {
			break;
		}
		// Check constraint
		if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
			throw ConstraintException("NOT NULL constraint failed: %s.%s", info->table,
			                          parent.column_definitions[not_null_constraint.index].GetName());
		}
	}
}

} // namespace duckdb
