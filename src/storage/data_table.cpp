#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

DataTable::DataTable(DatabaseInstance &db, shared_ptr<TableIOManager> table_io_manager_p, const string &schema,
                     const string &table, vector<ColumnDefinition> column_definitions_p,
                     unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(db, move(table_io_manager_p), schema, table)),
      column_definitions(move(column_definitions_p)), db(db), is_root(true) {
	// initialize the table with the existing data from disk, if any
	auto types = GetTypes();
	this->row_groups =
	    make_shared<RowGroupCollection>(info, TableIOManager::Get(*this).GetBlockManagerForRowData(), types, 0);
	if (data && !data->row_groups.empty()) {
		this->row_groups->Initialize(*data);
	} else {
		this->row_groups->InitializeEmpty();
		D_ASSERT(row_groups->GetTotalRows() == 0);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value)
    : info(parent.info), db(parent.db), is_root(true) {
	// add the column definitions from this DataTable
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	column_definitions.emplace_back(new_column.Copy());
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	this->row_groups = parent.row_groups->AddColumn(context, new_column, default_value);

	// also add this column to client local storage
	auto &local_storage = LocalStorage::Get(context);
	local_storage.AddColumn(&parent, this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), db(parent.db), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
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

	// erase the column definitions from this DataTable
	D_ASSERT(removed_column < column_definitions.size());
	column_definitions.erase(column_definitions.begin() + removed_column);

	storage_t storage_idx = 0;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		auto &col = column_definitions[i];
		col.SetOid(i);
		if (col.Generated()) {
			continue;
		}
		col.SetStorageOid(storage_idx++);
	}

	// alter the row_groups and remove the column from each of them
	this->row_groups = parent.row_groups->RemoveColumn(removed_column);

	// scan the original table, and fill the new column with the transformed value
	auto &local_storage = LocalStorage::Get(context);
	local_storage.DropColumn(&parent, this, removed_column);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

// Alter column to add new constraint
DataTable::DataTable(ClientContext &context, DataTable &parent, unique_ptr<BoundConstraint> constraint)
    : info(parent.info), db(parent.db), row_groups(parent.row_groups), is_root(true) {

	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	// Verify the new constraint against current persistent/local data
	VerifyNewConstraint(context, parent, constraint.get());

	// Get the local data ownership from old dt
	auto &local_storage = LocalStorage::Get(context);
	local_storage.MoveStorage(&parent, this);
	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     const vector<column_t> &bound_columns, Expression &cast_expr)
    : info(parent.info), db(parent.db), is_root(true) {
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
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
	column_definitions[changed_idx].SetType(target_type);

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	this->row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);

	// scan the original table, and fill the new column with the transformed value
	auto &local_storage = LocalStorage::Get(context);
	local_storage.ChangeType(&parent, this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

vector<LogicalType> DataTable::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : column_definitions) {
		types.push_back(it.Type());
	}
	return types;
}

TableIOManager &TableIOManager::Get(DataTable &table) {
	return *table.info->table_io_manager;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	state.Initialize(column_ids, table_filters);
	row_groups->InitializeScan(state.table_state, column_ids, table_filters);
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	auto &local_storage = LocalStorage::Get(transaction);
	local_storage.InitializeScan(this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
                                         idx_t end_row) {
	state.Initialize(column_ids);
	row_groups->InitializeScanWithOffset(state.table_state, column_ids, start_row, end_row);
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = RowGroup::ROW_GROUP_VECTOR_COUNT;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
	return GetTotalRows() / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	row_groups->InitializeParallelScan(state.scan_state);

	auto &local_storage = LocalStorage::Get(context);
	local_storage.InitializeParallelScan(this, state.local_state);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state) {
	if (row_groups->NextParallelScan(context, state.scan_state, scan_state.table_state)) {
		return true;
	}
	scan_state.table_state.batch_index = state.scan_state.batch_index;
	auto &local_storage = LocalStorage::Get(context);
	if (local_storage.NextParallelScan(context, this, state.local_state, scan_state.local_state)) {
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, TableScanState &state) {
	// scan the persistent segments
	if (state.table_state.Scan(transaction, result)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	auto &local_storage = LocalStorage::Get(transaction);
	local_storage.Scan(state.local_state, state.GetColumnIds(), result);
}

bool DataTable::CreateIndexScan(TableScanState &state, DataChunk &result, TableScanType type) {
	return state.table_state.ScanCommitted(result, type);
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
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (VectorOperations::HasNull(vector, count)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
	}
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                             Expression &expr, column_t index) {
	auto &col = table.columns.GetColumn(LogicalIndex(index));
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(context, expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		// FIXME: should this be this lenient? assertions are also caught this way
		throw ConstraintException("Incorrect value for generated column \"%s %s AS (%s)\" : %s", col.Name(),
		                          col.Type().ToString(), col.GeneratedExpression().ToString(), ex.what());
	}
}

static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr,
                                  DataChunk &chunk) {
	ExpressionExecutor executor(context, expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, ex.what());
	} catch (...) { // LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	} // LCOV_EXCL_STOP
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(chunk.size(), vdata);

	auto dataptr = (int32_t *)vdata.data;
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name);
		}
	}
}

bool DataTable::IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, Index &index, ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.column_ids.size()) {
		return false;
	}
	for (auto &fk_key : fk_keys) {
		bool is_found = false;
		for (auto &index_key : index.column_ids) {
			if (fk_key.index == index_key) {
				is_found = true;
				break;
			}
		}
		if (!is_found) {
			return false;
		}
	}
	return true;
}

idx_t LocateErrorIndex(bool is_append, SelectionVector &sel, idx_t match_count, idx_t count) {
	idx_t failed_index = DConstants::INVALID_INDEX;
	if (!is_append) {
		// We expected to find nothing, so the first error is the first match
		failed_index = sel.get_index(0);
	} else {
		// We expected to find a match for all of them, so the first non-match is the first error
		for (idx_t i = 0; i < match_count; i++) {
			if (sel.get_index(i) != i) {
				failed_index = i;
				break;
			}
		}
		if (failed_index == DConstants::INVALID_INDEX) {
			if (match_count == 0) {
				// No matches were found at all
				failed_index = match_count;
			} else if (match_count < count) {
				// The last couple of values did not find a match
				failed_index = match_count;
			}
		}
	}
	return failed_index;
}

[[noreturn]] static void ThrowForeignKeyConstraintError(idx_t failed_index, bool is_append, Index *index,
                                                        DataChunk &input) {
	auto verify_type = is_append ? VerifyExistenceType::APPEND_FK : VerifyExistenceType::DELETE_FK;

	D_ASSERT(failed_index != DConstants::INVALID_INDEX);
	D_ASSERT(index->type == IndexType::ART);
	auto &art_index = (ART &)*index;
	auto key_name = art_index.GenerateErrorKeyName(input, failed_index);
	auto exception_msg = art_index.GenerateConstraintErrorMessage(verify_type, move(key_name));
	throw ConstraintException(exception_msg);
}

bool IsForeignKeyConstraintError(bool is_append, idx_t input_count, idx_t match_count, idx_t nulls) {
	if (is_append) {
		// We need to find a match for all of the values
		return match_count != input_count;
	} else {
		// The only "matches" we should find are NULLs
		return match_count != nulls;
	}
}

static void VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context, DataChunk &chunk,
                                       bool is_append) {
	const vector<PhysicalIndex> *src_keys_ptr = &bfk.info.fk_keys;
	const vector<PhysicalIndex> *dst_keys_ptr = &bfk.info.pk_keys;
	if (!is_append) {
		src_keys_ptr = &bfk.info.pk_keys;
		dst_keys_ptr = &bfk.info.fk_keys;
	}

	auto table_entry_ptr =
	    Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, bfk.info.schema, bfk.info.table);
	if (table_entry_ptr == nullptr) {
		throw InternalException("Can't find table \"%s\" in foreign key constraint", bfk.info.table);
	}

	// make the data chunk to check
	vector<LogicalType> types;
	for (auto &col : table_entry_ptr->columns.Physical()) {
		types.emplace_back(col.Type());
	}
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr->size(); i++) {
		dst_chunk.data[(*dst_keys_ptr)[i].index].Reference(chunk.data[(*src_keys_ptr)[i].index]);
	}
	dst_chunk.SetCardinality(chunk.size());
	auto data_table = table_entry_ptr->storage.get();

	idx_t count = dst_chunk.size();
	if (count <= 0) {
		return;
	}

	// Contains all the indices that found a match with a foreign key
	SelectionVector regular_matches(count);
	SelectionVector transaction_matches(count);
	idx_t reg_null_count = 0;
	idx_t trans_null_count = 0;

	auto regular_match_count = data_table->info->indexes.VerifyForeignKey(*dst_keys_ptr, is_append, dst_chunk,
	                                                                      &regular_matches, reg_null_count);
	idx_t transaction_match_count = DConstants::INVALID_INDEX;
	// check whether or not the chunk can be inserted or deleted into the referenced table' transaction local storage
	auto &local_storage = LocalStorage::Get(context);

	bool error = IsForeignKeyConstraintError(is_append, count, regular_match_count, reg_null_count);
	bool transaction_error = false;

	bool transaction_check = local_storage.Find(data_table);
	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		transaction_match_count = transact_index.VerifyForeignKey(*dst_keys_ptr, is_append, dst_chunk,
		                                                          &transaction_matches, trans_null_count);
		transaction_error = IsForeignKeyConstraintError(is_append, count, transaction_match_count, trans_null_count);
	}

	if (!transaction_error && !error) {
		// No error occurred;
		return;
	}

	// Some error occurred, and we likely want to throw
	Index *index;
	Index *transaction_index;

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	index = data_table->info->indexes.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		// check whether or not the chunk can be inserted or deleted into the referenced table' storage
		transaction_index = transact_index.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	}

	if (!transaction_check) {
		// Only local state is checked, throw the error
		D_ASSERT(error);
		auto failed_index = LocateErrorIndex(is_append, regular_matches, regular_match_count, count);
		D_ASSERT(failed_index != DConstants::INVALID_INDEX);
		ThrowForeignKeyConstraintError(failed_index, is_append, index, dst_chunk);
	}
	if (transaction_error && error && is_append) {
		// When we want to do an append, we only throw if the foreign key does not exist in both transaction and local
		// storage
		idx_t failed_index = DConstants::INVALID_INDEX;
		idx_t regular_idx = 0;
		idx_t transaction_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			// If the index matches with that of the selection vector, increase the selection vector index
			bool regular_matches_left = regular_idx < regular_match_count bool in_regular =
			                                regular_matches_left && regular_matches.get_index(regular_idx) == i;
			if (in_regular) {
				regular_idx++;
			}
			bool transaction_matches_left = transaction_idx < transaction_match_count;
			bool in_transaction = transaction_matches_left && transaction_matches.get_index(transaction_idx) == i;
			if (in_transaction) {
				transaction_idx++;
			}
			if (!in_regular && !in_transaction) {
				// We need to find a match for all of the input values
				// The failed index is i, it does not show up in either regular or transaction storage
				failed_index = i;
				break;
			}
		}
		if (failed_index == DConstants::INVALID_INDEX) {
			// We don't throw, every value was present in either regular or transaction storage
			return;
		}
		ThrowForeignKeyConstraintError(failed_index, true, index, dst_chunk);
	}
	if (!is_append && transaction_check) {
		if (error) {
			auto failed_index = LocateErrorIndex(false, regular_matches, regular_match_count, count);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, index, dst_chunk);
		} else {
			D_ASSERT(transaction_error);
			D_ASSERT(transaction_match_count != DConstants::INVALID_INDEX);
			auto failed_index = LocateErrorIndex(false, transaction_matches, transaction_match_count, count);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, transaction_index, dst_chunk);
		}
	}
}

static void VerifyAppendForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                             DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, true);
}

static void VerifyDeleteForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                             DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, false);
}

void DataTable::VerifyNewConstraint(ClientContext &context, DataTable &parent, const BoundConstraint *constraint) {
	if (constraint->type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}

	parent.row_groups->VerifyNewConstraint(parent, *constraint);
	auto &local_storage = LocalStorage::Get(context);
	local_storage.VerifyNewConstraint(parent, *constraint);
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (table.HasGeneratedColumns()) {
		// Verify that the generated columns expression work with the inserted values
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder generated_check_binder(*binder, context, table.name, table.columns, bound_columns);
		for (auto &col : table.columns.Logical()) {
			if (!col.Generated()) {
				continue;
			}
			D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
			generated_check_binder.target_type = col.Type();
			auto to_be_bound_expression = col.GeneratedExpression().Copy();
			auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
			VerifyGeneratedExpressionSuccess(context, table, chunk, *bound_expression, col.Oid());
		}
	}
	for (idx_t i = 0; i < table.bound_constraints.size(); i++) {
		auto &base_constraint = table.constraints[i];
		auto &constraint = table.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			auto &not_null = *reinterpret_cast<NotNullConstraint *>(base_constraint.get());
			auto &col = table.columns.GetColumn(LogicalIndex(not_null.index));
			VerifyNotNullConstraint(table, chunk.data[bound_not_null.index.index], chunk.size(), col.Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(context, table, *check.expression, chunk);
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
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyAppendForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::InitializeLocalAppend(LocalAppendState &state, ClientContext &context) {
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	auto &local_storage = LocalStorage::Get(context);
	local_storage.InitializeAppend(state, this);
}

void DataTable::LocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                            DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	D_ASSERT(chunk.ColumnCount() == table.columns.PhysicalColumnCount());
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, context, chunk);
	// FIXME: call method that returns for which values the constraint check failed

	// append to the transaction local data
	LocalStorage::Append(state, chunk);
}

void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
	LocalStorage::FinalizeAppend(state);
}

OptimisticDataWriter *DataTable::CreateOptimisticWriter(ClientContext &context) {
	auto &local_storage = LocalStorage::Get(context);
	return local_storage.CreateOptimisticWriter(this);
}

void DataTable::LocalMerge(ClientContext &context, RowGroupCollection &collection) {
	auto &local_storage = LocalStorage::Get(context);
	local_storage.LocalMerge(this, collection);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	LocalAppendState append_state;
	table.storage->InitializeLocalAppend(append_state, context);
	table.storage->LocalAppend(append_state, table, context, chunk);
	table.storage->FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection) {
	LocalAppendState append_state;
	table.storage->InitializeLocalAppend(append_state, context);
	for (auto &chunk : collection.Chunks()) {
		table.storage->LocalAppend(append_state, table, context, chunk);
	}
	table.storage->FinalizeLocalAppend(append_state);
}

void DataTable::AppendLock(TableAppendState &state) {
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	state.row_start = row_groups->GetTotalRows();
	state.current_row = state.row_start;
}

void DataTable::InitializeAppend(Transaction &transaction, TableAppendState &state, idx_t append_count) {
	// obtain the append lock for this table
	if (!state.append_lock) {
		throw InternalException("DataTable::AppendLock should be called before DataTable::InitializeAppend");
	}
	row_groups->InitializeAppend(transaction, state, append_count);
}

void DataTable::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	row_groups->Append(chunk, state);
}

void DataTable::ScanTableSegment(idx_t row_start, idx_t count, const std::function<void(DataChunk &chunk)> &function) {
	idx_t end = row_start + count;

	vector<column_t> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->column_definitions.size(); i++) {
		auto &col = this->column_definitions[i];
		column_ids.push_back(i);
		types.push_back(col.Type());
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);

	CreateIndexScanState state;

	InitializeScanWithOffset(state, column_ids, row_start, row_start + count);
	auto row_start_aligned = state.table_state.row_group_state.row_group->start +
	                         state.table_state.row_group_state.vector_index * STANDARD_VECTOR_SIZE;

	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		state.table_state.ScanCommitted(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// start of chunk is current_row
		// end of chunk is end_row
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			D_ASSERT(chunk_count <= chunk.size());
			// need to slice the chunk before insert
			idx_t start_in_chunk;
			if (current_row >= row_start) {
				start_in_chunk = 0;
			} else {
				start_in_chunk = row_start - current_row;
			}
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}

void DataTable::MergeStorage(RowGroupCollection &data, TableIndexList &indexes) {
	row_groups->MergeStorage(data);
	row_groups->Verify();
}

void DataTable::WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count) {
	if (log.skip_writing) {
		return;
	}
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
		Vector row_identifiers(LogicalType::ROW_TYPE, (data_ptr_t)row_data);
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
bool DataTable::AppendToIndexes(TableIndexList &indexes, DataChunk &chunk, row_t row_start) {
	if (indexes.Empty()) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	vector<Index *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	indexes.Scan([&](Index &index) {
		try {
			if (!index.Append(chunk, row_identifiers)) {
				append_failed = true;
				return true;
			}
		} catch (...) {
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

bool DataTable::AppendToIndexes(DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	return AppendToIndexes(info->indexes, chunk, row_start);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
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
	row_groups->RemoveFromIndexes(info->indexes, row_identifiers, count);
}

void DataTable::VerifyDeleteConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyDeleteForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
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
	auto &local_storage = LocalStorage::Get(context);

	row_identifiers.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);
	auto first_id = ids[0];

	// verify any constraints on the delete rows
	// FIXME: we only need to fetch in case we have a foreign key constraint
	// and we only need to fetch columns that are part of this constraint
	DataChunk verify_chunk;
	if (first_id >= MAX_ROW_ID) {
		local_storage.FetchChunk(this, row_identifiers, count, verify_chunk);
	} else {
		ColumnFetchState fetch_state;
		vector<column_t> col_ids;
		vector<LogicalType> types;
		for (idx_t i = 0; i < column_definitions.size(); i++) {
			col_ids.push_back(column_definitions[i].StorageOid());
			types.emplace_back(column_definitions[i].Type());
		}
		verify_chunk.Initialize(Allocator::Get(context), types);
		Fetch(transaction, verify_chunk, col_ids, row_identifiers, count, fetch_state);
	}
	VerifyDeleteConstraints(table, context, verify_chunk);

	if (first_id >= MAX_ROW_ID) {
		// deletion is in transaction-local storage: push delete into local chunk collection
		return local_storage.Delete(this, row_identifiers, count);
	} else {
		return row_groups->Delete(transaction, this, ids, count);
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, const vector<PhysicalIndex> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i].index].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, const vector<PhysicalIndex> &column_ids,
                            physical_index_set_t &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
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

void DataTable::VerifyUpdateConstraints(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                        const vector<PhysicalIndex> &column_ids) {
	for (idx_t i = 0; i < table.bound_constraints.size(); i++) {
		auto &base_constraint = table.constraints[i];
		auto &constraint = table.bound_constraints[i];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			auto &not_null = *reinterpret_cast<NotNullConstraint *>(base_constraint.get());
			// check if the constraint is in the list of column_ids
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] == bound_not_null.index) {
					// found the column id: check the data in
					auto &col = table.columns.GetColumn(LogicalIndex(not_null.index));
					VerifyNotNullConstraint(table, chunk.data[i], chunk.size(), col.Name());
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(context, table, *check.expression, mock_chunk);
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
                       const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
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
	VerifyUpdateConstraints(context, table, updates, column_ids);

	// now perform the actual update
	auto &transaction = Transaction::GetTransaction(context);

	updates.Flatten();
	row_ids.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		// update is in transaction-local storage: push update into local storage
		auto &local_storage = LocalStorage::Get(context);
		local_storage.Update(this, row_ids, column_ids, updates);
		return;
	}

	// update is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	row_groups->Update(transaction, ids, column_ids, updates);
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

	updates.Flatten();
	row_ids.Flatten(updates.size());
	row_groups->UpdateColumn(transaction, row_ids, column_path, updates);
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

unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return row_groups->CopyStats(column_id);
}

void DataTable::SetStatistics(column_t column_id, const std::function<void(BaseStatistics &)> &set_fun) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	row_groups->SetStatistics(column_id, set_fun);
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void DataTable::Checkpoint(TableDataWriter &writer) {
	// checkpoint each individual row group
	// FIXME: we might want to combine adjacent row groups in case they have had deletions...
	vector<unique_ptr<BaseStatistics>> global_stats;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		global_stats.push_back(row_groups->CopyStats(i));
	}

	row_groups->Checkpoint(writer, global_stats);

	// The rowgroup payload data has been written. Now write:
	//   column stats
	//   row-group pointers
	//   table pointer
	//   index data
	writer.FinalizeTable(move(global_stats), info.get());
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
