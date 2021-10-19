#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"

#include "duckdb/common/chrono.hpp"

namespace duckdb {

DataTable::DataTable(DatabaseInstance &db, const string &schema, const string &table, vector<LogicalType> types_p,
                     unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(db, schema, table)), types(move(types_p)), db(db), is_root(true) {
	// initialize the table with the existing data from disk, if any
	this->row_groups = make_shared<RowGroupCollection>(info, types, 0);
	if (data && !data->row_groups.empty()) {
		this->row_groups->Initialize(*data);
		stats.Initialize(types, *data);
	}
	if (stats.Empty()) {
		D_ASSERT(row_groups->GetTotalRows() == 0);

		row_groups->InitializeEmpty();
		stats.InitializeEmpty(types);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value)
    : info(parent.info), types(parent.types), db(parent.db), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);
	// add the new column to this DataTable
	auto new_column_type = new_column.type;
	auto new_column_idx = parent.types.size();

	types.push_back(new_column_type);
	stats.InitializeAddColumn(parent.stats, new_column_type);

	auto &transaction = Transaction::GetTransaction(context);
	this->row_groups = parent.row_groups->AddColumn(new_column, default_value, stats.GetStats(new_column_idx));

	// also add this column to client local storage
	transaction.storage.AddColumn(&parent, this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), types(parent.types), db(parent.db), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);
	// first check if there are any indexes that exist that point to the removed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on it!");
			} else if (column_id > removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on a column after it!");
			}
		}
		return false;
	});

	// erase the stats and type from this DataTable
	D_ASSERT(removed_column < types.size());
	types.erase(types.begin() + removed_column);
	stats.InitializeRemoveColumn(parent.stats, removed_column);

	// alter the row_groups and remove the column from each of them
	this->row_groups = parent.row_groups->RemoveColumn(removed_column);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     vector<column_t> bound_columns, Expression &cast_expr)
    : info(parent.info), types(parent.types), db(parent.db), is_root(true) {
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);

	// first check if there are any indexes that exist that point to the changed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == changed_idx) {
				throw CatalogException("Cannot change the type of this column: an index depends on it!");
			}
		}
		return false;
	});

	// change the type in this DataTable
	types[changed_idx] = target_type;

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	stats.InitializeAlterType(parent.stats, changed_idx, target_type);
	this->row_groups =
	    parent.row_groups->AlterType(changed_idx, target_type, bound_columns, cast_expr, stats.GetStats(changed_idx));

	// scan the original table, and fill the new column with the transformed value
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.ChangeType(&parent, this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	row_groups->InitializeScan(state, column_ids, table_filters);
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	transaction.storage.InitializeScan(this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
                                         idx_t end_row) {
	row_groups->InitializeScanWithOffset(state, column_ids, start_row, end_row);
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = RowGroup::ROW_GROUP_VECTOR_COUNT;
	if (context.verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
	return GetTotalRows() / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	row_groups->InitializeParallelScan(context, state);
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.InitializeScan(this, state.local_state, nullptr);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
                                 const vector<column_t> &column_ids) {
	while (row_groups->NextParallelScan(context, state, scan_state, column_ids)) {
		return true;
	}
	if (!state.transaction_local_data) {
		auto &transaction = Transaction::GetTransaction(context);
		// create a task for scanning the local data
		scan_state.row_group_scan_state.max_row = 0;
		scan_state.max_row = 0;
		transaction.storage.InitializeScan(this, scan_state.local_state, scan_state.table_filters);
		scan_state.local_state.max_index = state.local_state.max_index;
		scan_state.local_state.last_chunk_count = state.local_state.last_chunk_count;
		state.transaction_local_data = true;
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, TableScanState &state, vector<column_t> &column_ids) {
	// scan the persistent segments
	if (ScanBaseTable(transaction, result, state)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	transaction.storage.Scan(state.local_state, column_ids, result);
}

bool DataTable::ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state) {
	auto current_row_group = state.row_group_scan_state.row_group;
	while (current_row_group) {
		current_row_group->Scan(transaction, state.row_group_scan_state, result);
		if (result.size() > 0) {
			return true;
		} else {
			do {
				current_row_group = state.row_group_scan_state.row_group = (RowGroup *)current_row_group->next.get();
				if (current_row_group) {
					bool scan_row_group = current_row_group->InitializeScan(state.row_group_scan_state);
					if (scan_row_group) {
						// skip this row group
						break;
					}
				}
			} while (current_row_group);
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                      Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, string &col_name) {
	if (VectorOperations::HasNull(vector, count)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
	}
}

static void VerifyCheckConstraint(TableCatalogEntry &table, Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, ex.what());
	} catch (...) { // LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	} // LCOV_EXCL_STOP
	VectorData vdata;
	result.Orrify(chunk.size(), vdata);

	auto dataptr = (int32_t *)vdata.data;
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name);
		}
	}
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			VerifyNotNullConstraint(table, chunk.data[not_null.index], chunk.size(),
			                        table.columns[not_null.index].name);
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			//! check whether or not the chunk can be inserted into the indexes
			info->indexes.Scan([&](Index &index) {
				index.VerifyAppend(chunk);
				return false;
			});
			break;
		}
		case ConstraintType::FOREIGN_KEY:
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	if (chunk.ColumnCount() != table.columns.size()) {
		throw InternalException("Mismatch in column count for append");
	}
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, chunk);

	// append to the transaction local data
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.Append(this, chunk);
}

void DataTable::InitializeAppend(Transaction &transaction, TableAppendState &state, idx_t append_count) {
	// obtain the append lock for this table
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	state.remaining_append_count = append_count;

	row_groups->InitializeAppend(transaction, state, append_count);
}

void DataTable::Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	row_groups->Append(transaction, chunk, state, stats);
}

void DataTable::ScanTableSegment(idx_t row_start, idx_t count, const std::function<void(DataChunk &chunk)> &function) {
	idx_t end = row_start + count;

	vector<column_t> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->types.size(); i++) {
		column_ids.push_back(i);
		types.push_back(this->types[i]);
	}
	DataChunk chunk;
	chunk.Initialize(types);

	CreateIndexScanState state;

	idx_t row_start_aligned = row_start / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
	InitializeScanWithOffset(state, column_ids, row_start_aligned, row_start + count);

	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		ScanCreateIndex(state, chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			// need to slice the chunk before insert
			auto start_in_chunk = chunk_start % STANDARD_VECTOR_SIZE;
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}

void DataTable::WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count) {
	log.WriteSetTable(info->schema, info->table);
	ScanTableSegment(row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);
	row_groups->CommitAppend(commit_id, row_start, count);
	info->cardinality += count;
}

void DataTable::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (count == 0) {
		// nothing to revert!
		return;
	}
	// adjust the cardinality
	info->cardinality = start_row;
	D_ASSERT(is_root);
	// revert appends made to row_groups
	row_groups->RevertAppendInternal(start_row, count);
}

void DataTable::RevertAppend(idx_t start_row, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	if (!info->indexes.Empty()) {
		idx_t current_row_base = start_row;
		row_t row_data[STANDARD_VECTOR_SIZE];
		Vector row_identifiers(LOGICAL_ROW_TYPE, (data_ptr_t)row_data);
		ScanTableSegment(start_row, count, [&](DataChunk &chunk) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				row_data[i] = current_row_base + i;
			}
			info->indexes.Scan([&](Index &index) {
				index.Delete(chunk, row_identifiers);
				return false;
			});
			current_row_base += chunk.size();
		});
	}
	RevertAppendInternal(start_row, count);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
bool DataTable::AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LOGICAL_ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	vector<Index *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	info->indexes.Scan([&](Index &index) {
		if (!index.Append(chunk, row_identifiers)) {
			append_failed = true;
			return true;
		}
		already_appended.push_back(&index);
		return false;
	});

	if (append_failed) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)

		for (auto *index : already_appended) {
			index->Delete(chunk, row_identifiers);
		}

		return false;
	}
	return true;
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LOGICAL_ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	// now remove the entries from the indices
	RemoveFromIndexes(state, chunk, row_identifiers);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers) {
	D_ASSERT(is_root);
	info->indexes.Scan([&](Index &index) {
		index.Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	D_ASSERT(is_root);
	row_groups->RemoveFromIndexes(row_identifiers, count);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
idx_t DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return 0;
	}

	auto &transaction = Transaction::GetTransaction(context);

	row_identifiers.Normalify(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);
	auto first_id = ids[0];

	if (first_id >= MAX_ROW_ID) {
		// deletion is in transaction-local storage: push delete into local chunk collection
		return transaction.storage.Delete(this, row_identifiers, count);
	} else {
		return row_groups->Delete(transaction, this, ids, count);
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, const vector<column_t> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i]].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, const vector<column_t> &column_ids,
                            unordered_set<column_t> &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	idx_t found_columns = 0;
	// check whether the desired columns are present in the UPDATE clause
	for (column_t i = 0; i < column_ids.size(); i++) {
		if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
			found_columns++;
		}
	}
	if (found_columns == 0) {
		// no columns were found: no need to check the constraint again
		return false;
	}
	if (found_columns != desired_column_ids.size()) {
		// not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw InternalException("Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	auto types = table.GetTypes();
	CreateMockChunk(types, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk,
                                        const vector<column_t> &column_ids) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			// check if the constraint is in the list of column_ids
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] == not_null.index) {
					// found the column id: check the data in
					VerifyNotNullConstraint(table, chunk.data[i], chunk.size(), table.columns[not_null.index].name);
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(table, *check.expression, mock_chunk);
			}
			break;
		}
		case ConstraintType::UNIQUE:
		case ConstraintType::FOREIGN_KEY:
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	// update should not be called for indexed columns!
	// instead update should have been rewritten to delete + update on higher layer
#ifdef DEBUG
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(!index.IndexIsUpdated(column_ids));
		return false;
	});

#endif
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                       const vector<column_t> &column_ids, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);

	auto count = updates.size();
	updates.Verify();
	if (count == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(table, updates, column_ids);

	// now perform the actual update
	auto &transaction = Transaction::GetTransaction(context);

	updates.Normalify();
	row_ids.Normalify(count);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		// update is in transaction-local storage: push update into local storage
		transaction.storage.Update(this, row_ids, column_ids, updates);
		return;
	}

	// update is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	row_groups->Update(transaction, ids, column_ids, updates, stats);
}

void DataTable::UpdateColumn(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                             const vector<column_t> &column_path, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(updates.ColumnCount() == 1);
	updates.Verify();
	if (updates.size() == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// now perform the actual update
	auto &transaction = Transaction::GetTransaction(context);

	updates.Normalify();
	row_ids.Normalify(updates.size());
	row_groups->UpdateColumn(transaction, row_ids, column_path, updates, stats);
}

//===--------------------------------------------------------------------===//
// Create Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids) {
	// we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	state.append_lock = std::unique_lock<mutex>(append_lock);
	row_groups->InitializeCreateIndexScan(state);
	InitializeScan(state, column_ids);
}

bool DataTable::ScanCreateIndex(CreateIndexScanState &state, DataChunk &result, TableScanType type) {
	auto current_row_group = state.row_group_scan_state.row_group;
	while (current_row_group) {
		current_row_group->ScanCommitted(state.row_group_scan_state, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			current_row_group = state.row_group_scan_state.row_group = (RowGroup *)current_row_group->next.get();
			if (current_row_group) {
				current_row_group->InitializeScan(state.row_group_scan_state);
			}
		}
	}
	return false;
}

void DataTable::AddIndex(unique_ptr<Index> index, const vector<unique_ptr<Expression>> &expressions) {
	DataChunk result;
	result.Initialize(index->logical_types);

	DataChunk intermediate;
	vector<LogicalType> intermediate_types;
	auto column_ids = index->column_ids;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	for (auto &id : index->column_ids) {
		intermediate_types.push_back(types[id]);
	}
	intermediate_types.push_back(LOGICAL_ROW_TYPE);
	intermediate.Initialize(intermediate_types);

	// initialize an index scan
	CreateIndexScanState state;
	InitializeCreateIndexScan(state, column_ids);

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	// now start incrementally building the index
	{
		IndexLock lock;
		index->InitializeLock(lock);
		ExpressionExecutor executor(expressions);
		while (true) {
			intermediate.Reset();
			// scan a new chunk from the table to index
			ScanCreateIndex(state, intermediate, TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
			if (intermediate.size() == 0) {
				// finished scanning for index creation
				// release all locks
				break;
			}
			// resolve the expressions for this chunk
			executor.Execute(intermediate, result);

			// insert into the index
			if (!index->Insert(lock, result, intermediate.data[intermediate.ColumnCount() - 1])) {
				throw ConstraintException(
				    "Cant create unique index, table contains duplicate data on indexed column(s)");
			}
		}
	}
	info->indexes.AddIndex(move(index));
}

unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return stats.CopyStats(column_id);
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
BlockPointer DataTable::Checkpoint(TableDataWriter &writer) {
	// checkpoint each individual row group
	// FIXME: we might want to combine adjacent row groups in case they have had deletions...
	vector<unique_ptr<BaseStatistics>> global_stats;
	for (idx_t i = 0; i < types.size(); i++) {
		global_stats.push_back(BaseStatistics::CreateEmpty(types[i]));
	}

	vector<RowGroupPointer> row_group_pointers;
	row_groups->Checkpoint(writer, row_group_pointers, global_stats);

	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto &meta_writer = writer.GetMetaWriter();
	auto pointer = meta_writer.GetBlockPointer();

	for (auto &stats : global_stats) {
		stats->Serialize(meta_writer);
	}
	// now start writing the row group pointers to disk
	meta_writer.Write<uint64_t>(row_group_pointers.size());
	for (auto &row_group_pointer : row_group_pointers) {
		RowGroup::Serialize(row_group_pointer, meta_writer);
	}
	return pointer;
}

void DataTable::CommitDropColumn(idx_t index) {
	row_groups->CommitDropColumn(index);
}

idx_t DataTable::GetTotalRows() {
	return row_groups->GetTotalRows();
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified so they can be reclaimed later on
	row_groups->CommitDropTable();
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
vector<vector<Value>> DataTable::GetStorageInfo() {
	return row_groups->GetStorageInfo();
}

} // namespace duckdb
