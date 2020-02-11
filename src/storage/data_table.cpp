#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/table/transient_segment.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types_,
                     unique_ptr<vector<unique_ptr<PersistentSegment>>[]> data)
    : cardinality(0), schema(schema), table(table), types(types_), storage(storage), persistent_manager(*this),
      transient_manager(*this) {
	// set up the segment trees for the column segments
	columns = unique_ptr<ColumnData[]>(new ColumnData[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].type = types[i];
		columns[i].table = this;
		columns[i].column_idx = i;
	}

	// initialize the table with the existing data from disk, if any
	if (data && data[0].size() > 0) {
		// first append all the segments to the set of column segments
		for (index_t i = 0; i < types.size(); i++) {
			columns[i].Initialize(data[i]);
			if (columns[i].persistent_rows != columns[0].persistent_rows) {
				throw Exception("Column length mismatch in table load!");
			}
		}
		persistent_manager.max_row = columns[0].persistent_rows;
		transient_manager.base_row = persistent_manager.max_row;
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, vector<column_t> column_ids) {
	// initialize a column scan state for each column
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (index_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column].InitializeScan(state.column_scans[i]);
		}
	}
	state.column_ids = move(column_ids);
	// initialize the chunk scan state
	state.offset = 0;
	state.current_persistent_row = 0;
	state.max_persistent_row = persistent_manager.max_row;
	state.current_transient_row = 0;
	state.max_transient_row = transient_manager.max_row;
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, vector<column_t> column_ids) {
	InitializeScan(state, move(column_ids));
	transaction.storage.InitializeScan(this, state.local_state);
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, TableScanState &state) {
	// scan the persistent segments
	while (ScanBaseTable(transaction, result, state, state.current_persistent_row, state.max_persistent_row, 0,
	                     persistent_manager)) {
		if (result.size() > 0) {
			return;
		}
	}
	// scan the transient segments
	while (ScanBaseTable(transaction, result, state, state.current_transient_row, state.max_transient_row,
	                     persistent_manager.max_row, transient_manager)) {
		if (result.size() > 0) {
			return;
		}
	}

	// scan the transaction-local segments
	transaction.storage.Scan(state.local_state, state.column_ids, result);
}

bool DataTable::ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state, index_t &current_row,
                              index_t max_row, index_t base_row, VersionManager &manager) {
	if (current_row >= max_row) {
		// exceeded the amount of rows to scan
		return false;
	}
	index_t max_count = std::min((index_t)STANDARD_VECTOR_SIZE, max_row - current_row);
	index_t vector_offset = current_row / STANDARD_VECTOR_SIZE;
	// first scan the version chunk manager to figure out which tuples to load for this transaction
	index_t count = manager.GetSelVector(transaction, vector_offset, state.sel_vector, max_count);
	if (count == 0) {
		// nothing to scan for this vector, skip the entire vector
		for (index_t i = 0; i < state.column_ids.size(); i++) {
			auto column = state.column_ids[i];
			if (column != COLUMN_IDENTIFIER_ROW_ID) {
				state.column_scans[i].Next();
			}
		}
		current_row += STANDARD_VECTOR_SIZE;
		return true;
	}

	sel_t *sel_vector = count == max_count ? nullptr : state.sel_vector;
	// now scan the base columns to fetch the actual data
	for (index_t i = 0; i < state.column_ids.size(); i++) {
		auto column = state.column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// scan row id
			assert(result.data[i].type == ROW_TYPE);
			result.data[i].Sequence(base_row + current_row, 1, max_count);
		} else {
			// scan actual base column
			columns[column].Scan(transaction, state.column_scans[i], result.data[i]);
		}
		result.data[i].sel_vector = sel_vector;
		result.data[i].count = count;
	}
	result.sel_vector = sel_vector;

	current_row += STANDARD_VECTOR_SIZE;
	return true;
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index,
                                    vector<column_t> column_ids) {
	state.index = &index;
	state.column_ids = move(column_ids);
	transaction.storage.InitializeScan(this, state.local_state);
}

void DataTable::InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index, Value value,
                                    ExpressionType expr_type, vector<column_t> column_ids) {
	InitializeIndexScan(transaction, state, index, move(column_ids));
	state.index_state = index.InitializeScanSinglePredicate(transaction, state.column_ids, value, expr_type);
}

void DataTable::InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index, Value low_value,
                                    ExpressionType low_type, Value high_value, ExpressionType high_type,
                                    vector<column_t> column_ids) {
	InitializeIndexScan(transaction, state, index, move(column_ids));
	state.index_state =
	    index.InitializeScanTwoPredicates(transaction, state.column_ids, low_value, low_type, high_value, high_type);
}

void DataTable::IndexScan(Transaction &transaction, DataChunk &result, TableIndexScanState &state) {
	// clear any previously pinned blocks
	state.fetch_state.handles.clear();
	// scan the index
	state.index->Scan(transaction, state, result);
	if (result.size() > 0) {
		return;
	}
	// scan the local structure
	transaction.storage.Scan(state.local_state, state.column_ids, result);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                      Vector &row_identifiers, TableIndexScanState &state) {
	// first figure out which row identifiers we should use for this transaction by looking at the VersionManagers
	row_t rows[STANDARD_VECTOR_SIZE];
	index_t count = FetchRows(transaction, row_identifiers, rows);

	if (count == 0) {
		// no rows to use
		return;
	}
	// for each of the remaining rows, now fetch the data
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			assert(result.data[col_idx].type == TypeId::INT64);
			result.data[col_idx].count = count;
			auto data = (row_t *)result.data[col_idx].GetData();
			for (index_t i = 0; i < count; i++) {
				data[i] = rows[i];
			}
		} else {
			// regular column: fetch data from the base column
			for (index_t i = 0; i < count; i++) {
				auto row_id = rows[i];
				columns[column].FetchRow(state.fetch_state, transaction, row_id, result.data[col_idx]);
			}
		}
	}
}

index_t DataTable::FetchRows(Transaction &transaction, Vector &row_identifiers, row_t result_rows[]) {
	assert(row_identifiers.type == ROW_TYPE);

	// obtain a read lock on the version managers
	auto l1 = persistent_manager.lock.GetSharedLock();
	auto l2 = transient_manager.lock.GetSharedLock();

	// now iterate over the row ids and figure out which rows to use
	index_t count = 0;

	auto row_ids = (row_t *)row_identifiers.GetData();
	VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
		auto row_id = row_ids[i];
		bool use_row;
		if ((index_t)row_id < persistent_manager.max_row) {
			// persistent row: use persistent manager
			use_row = persistent_manager.Fetch(transaction, row_id);
		} else {
			// transient row: use transient manager
			use_row = transient_manager.Fetch(transaction, row_id);
		}
		if (use_row) {
			// row is not deleted; use the row
			result_rows[count++] = row_id;
		}
	});
	return count;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, string &col_name) {
	if (VectorOperations::HasNull(vector)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name.c_str(), col_name.c_str());
	}
}

static void VerifyCheckConstraint(TableCatalogEntry &table, Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(expr);
	Vector result(TypeId::INT32, true, false);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (Exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name.c_str(), ex.what());
	} catch (...) {
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name.c_str());
	}

	auto dataptr = (int *)result.GetData();
	for (index_t i = 0; i < result.count; i++) {
		index_t index = result.sel_vector ? result.sel_vector[i] : i;
		if (!result.nullmask[index] && dataptr[index] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name.c_str());
		}
	}
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			VerifyNotNullConstraint(table, chunk.data[not_null.index], table.columns[not_null.index].name);
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			//! check whether or not the chunk can be inserted into the indexes
			for (auto &index : indexes) {
				index->VerifyAppend(chunk);
			}
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
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, chunk);

	// append to the transaction local data
	auto &transaction = context.ActiveTransaction();
	transaction.storage.Append(this, chunk);
}

void DataTable::InitializeAppend(TableAppendState &state) {
	// obtain the append lock for this table
	state.append_lock = unique_lock<mutex>(append_lock);
	// obtain locks on all indexes for the table
	state.index_locks = unique_ptr<IndexLock[]>(new IndexLock[indexes.size()]);
	for (index_t i = 0; i < indexes.size(); i++) {
		indexes[i]->InitializeLock(state.index_locks[i]);
	}
	// for each column, initialize the append state
	state.states = unique_ptr<ColumnAppendState[]>(new ColumnAppendState[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].InitializeAppend(state.states[i]);
	}
	state.row_start = transient_manager.max_row;
	state.current_row = state.row_start;
}

void DataTable::Append(Transaction &transaction, transaction_t commit_id, DataChunk &chunk, TableAppendState &state) {
	assert(chunk.column_count == types.size());
	chunk.Verify();

	// set up the inserted info in the version manager
	transient_manager.Append(transaction, state.current_row, chunk.size(), commit_id);

	// append the physical data to each of the entries
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].Append(state.states[i], chunk.data[i]);
	}
	cardinality += chunk.size();
	state.current_row += chunk.size();
}

void DataTable::RevertAppend(TableAppendState &state) {
	if (state.row_start == state.current_row) {
		// nothing to revert!
		return;
	}
	// revert changes in the base columns
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].RevertAppend(state.row_start);
	}
	// adjust the cardinality
	cardinality -= state.current_row - state.row_start;
	transient_manager.max_row = state.row_start;
	// revert changes in the transient manager
	transient_manager.RevertAppend(state.row_start, state.current_row);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
bool DataTable::AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	if (indexes.size() == 0) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(ROW_TYPE);
	row_identifiers.sel_vector = chunk.sel_vector;
	row_identifiers.count = chunk.size();
	VectorOperations::GenerateSequence(row_identifiers, row_start, 1, true);

	index_t failed_index = INVALID_INDEX;
	// now append the entries to the indices
	for (index_t i = 0; i < indexes.size(); i++) {
		if (!indexes[i]->Append(state.index_locks[i], chunk, row_identifiers)) {
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (index_t i = 0; i < failed_index; i++) {
			indexes[i]->Delete(state.index_locks[i], chunk, row_identifiers);
		}
		return false;
	}
	return true;
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	if (indexes.size() == 0) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(ROW_TYPE);
	row_identifiers.sel_vector = chunk.sel_vector;
	row_identifiers.count = chunk.size();
	VectorOperations::GenerateSequence(row_identifiers, row_start, 1, true);

	// now remove the entries from the indices
	RemoveFromIndexes(state, chunk, row_identifiers);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers) {
	for (index_t i = 0; i < indexes.size(); i++) {
		indexes[i]->Delete(state.index_locks[i], chunk, row_identifiers);
	}
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers) {
	assert(!row_identifiers.sel_vector);
	auto row_ids = (row_t *)row_identifiers.GetData();
	// create a selection vector from the row_ids
	sel_t sel[STANDARD_VECTOR_SIZE];
	for (index_t i = 0; i < row_identifiers.count; i++) {
		sel[i] = row_ids[i] % STANDARD_VECTOR_SIZE;
	}

	// fetch the data for these row identifiers
	DataChunk result;
	result.Initialize(types);
	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	auto states = unique_ptr<ColumnScanState[]>(new ColumnScanState[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].Fetch(states[i], row_ids[0], result.data[i]);
		result.data[i].count = row_identifiers.count;
		result.data[i].sel_vector = sel;
	}
	result.sel_vector = sel;
	for (index_t i = 0; i < indexes.size(); i++) {
		indexes[i]->Delete(result, row_identifiers);
	}
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers) {
	assert(row_identifiers.type == ROW_TYPE);
	if (row_identifiers.count == 0) {
		return;
	}

	Transaction &transaction = context.ActiveTransaction();

	row_identifiers.Normalify();
	auto ids = (row_t *)row_identifiers.GetData();
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];

	if (first_id >= MAX_ROW_ID) {
		// deletion is in transaction-local storage: push delete into local chunk collection
		transaction.storage.Delete(this, row_identifiers);
	} else if ((index_t)first_id < persistent_manager.max_row) {
		// deletion is in persistent storage: delete in the persistent version manager
		persistent_manager.Delete(transaction, row_identifiers);
	} else {
		// deletion is in transient storage: delete in the persistent version manager
		transient_manager.Delete(transaction, row_identifiers);
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<TypeId> &types, vector<column_t> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i]].Reference(chunk.data[i]);
		mock_chunk.sel_vector = mock_chunk.data[column_ids[i]].sel_vector;
	}
	mock_chunk.data[0].count = chunk.size();
}

static bool CreateMockChunk(TableCatalogEntry &table, vector<column_t> &column_ids,
                            unordered_set<column_t> &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	index_t found_columns = 0;
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
		// FIXME: not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw NotImplementedException(
		    "Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	auto types = table.GetTypes();
	CreateMockChunk(types, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			// check if the constraint is in the list of column_ids
			for (index_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] == not_null.index) {
					// found the column id: check the data in
					VerifyNotNullConstraint(table, chunk.data[i], table.columns[not_null.index].name);
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
	for (index_t i = 0; i < indexes.size(); i++) {
		assert(!indexes[i]->IndexIsUpdated(column_ids));
	}
#endif
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers,
                       vector<column_t> &column_ids, DataChunk &updates) {
	assert(row_identifiers.type == ROW_TYPE);
	assert(updates.sel_vector == row_identifiers.sel_vector);
	assert(updates.size() == row_identifiers.count);

	updates.Verify();
	if (row_identifiers.count == 0) {
		return;
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(table, updates, column_ids);

	// now perform the actual update
	Transaction &transaction = context.ActiveTransaction();

	auto ids = (row_t *)row_identifiers.GetData();
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];

	if (first_id >= MAX_ROW_ID) {
		// update is in transaction-local storage: push update into local storage
		transaction.storage.Update(this, row_identifiers, column_ids, updates);
		return;
	}

	for (index_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		assert(column != COLUMN_IDENTIFIER_ROW_ID);

		columns[column].Update(transaction, updates.data[i], ids);
	}
}

//===--------------------------------------------------------------------===//
// Create Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeCreateIndexScan(CreateIndexScanState &state, vector<column_t> column_ids) {
	// we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	state.append_lock = unique_lock<mutex>(append_lock);
	// get a read lock on the VersionManagers to prevent any further deletions
	state.locks.push_back(persistent_manager.lock.GetSharedLock());
	state.locks.push_back(transient_manager.lock.GetSharedLock());

	InitializeScan(state, column_ids);
}

void DataTable::CreateIndexScan(CreateIndexScanState &state, DataChunk &result) {
	// scan the persistent segments
	if (ScanCreateIndex(state, result, state.current_persistent_row, state.max_persistent_row, 0)) {
		return;
	}
	// scan the transient segments
	if (ScanCreateIndex(state, result, state.current_transient_row, state.max_transient_row,
	                    state.max_persistent_row)) {
		return;
	}
}

bool DataTable::ScanCreateIndex(CreateIndexScanState &state, DataChunk &result, index_t &current_row, index_t max_row,
                                index_t base_row) {
	if (current_row >= max_row) {
		return false;
	}
	index_t count = std::min((index_t)STANDARD_VECTOR_SIZE, max_row - current_row);

	// scan the base columns to fetch the actual data
	// note that we insert all data into the index, even if it is marked as deleted
	// FIXME: tuples that are already "cleaned up" do not need to be inserted into the index!
	for (index_t i = 0; i < state.column_ids.size(); i++) {
		auto column = state.column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// scan row id
			assert(result.data[i].type == ROW_TYPE);
			result.data[i].Sequence(base_row + current_row, 1, count);
		} else {
			// scan actual base column
			columns[column].IndexScan(state.column_scans[i], result.data[i]);
		}
		result.data[i].count = count;
	}

	current_row += STANDARD_VECTOR_SIZE;
	return count > 0;
}

void DataTable::AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions) {
	DataChunk result;
	result.Initialize(index->types);

	DataChunk intermediate;
	vector<TypeId> intermediate_types;
	auto column_ids = index->column_ids;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	for (auto &id : index->column_ids) {
		intermediate_types.push_back(types[id]);
	}
	intermediate_types.push_back(ROW_TYPE);
	intermediate.Initialize(intermediate_types);

	// initialize an index scan
	CreateIndexScanState state;
	InitializeCreateIndexScan(state, column_ids);

	// now start incrementally building the index
	IndexLock lock;
	index->InitializeLock(lock);
	ExpressionExecutor executor(expressions);
	while (true) {
		intermediate.Reset();
		// scan a new chunk from the table to index
		CreateIndexScan(state, intermediate);
		if (intermediate.size() == 0) {
			// finished scanning for index creation
			// release all locks
			break;
		}
		// resolve the expressions for this chunk
		executor.Execute(intermediate, result);

		// insert into the index
		if (!index->Insert(lock, result, intermediate.data[intermediate.column_count - 1])) {
			throw ConstraintException("Cant create unique index, table contains duplicate data on indexed column(s)");
		}
	}
	indexes.push_back(move(index));
}

bool DataTable::IsTemporary() {
	return schema.compare(TEMP_SCHEMA) == 0;
}
