#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

namespace duckdb {

DataTableInfo::DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, string schema,
                             string table)
    : db(db), table_io_manager(std::move(table_io_manager_p)), schema(std::move(schema)), table(std::move(table)) {
}

void DataTableInfo::InitializeIndexes(ClientContext &context, const char *index_type) {
	indexes.InitializeIndexes(context, *this, index_type);
}

bool DataTableInfo::IsTemporary() const {
	return db.IsTemporary();
}

DataTable::DataTable(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, const string &schema,
                     const string &table, vector<ColumnDefinition> column_definitions_p,
                     unique_ptr<PersistentTableData> data)
    : db(db), info(make_shared_ptr<DataTableInfo>(db, std::move(table_io_manager_p), schema, table)),
      column_definitions(std::move(column_definitions_p)), is_root(true) {
	// initialize the table with the existing data from disk, if any
	auto types = GetTypes();
	auto &io_manager = TableIOManager::Get(*this);
	this->row_groups = make_shared_ptr<RowGroupCollection>(info, io_manager, types, 0);
	if (data && data->row_group_count > 0) {
		this->row_groups->Initialize(*data);
	} else {
		this->row_groups->InitializeEmpty();
		D_ASSERT(row_groups->GetTotalRows() == 0);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression &default_value)
    : db(parent.db), info(parent.info), is_root(true) {
	// add the column definitions from this DataTable
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	column_definitions.emplace_back(new_column.Copy());

	auto &local_storage = LocalStorage::Get(context, db);

	ExpressionExecutor default_executor(context);
	default_executor.AddExpression(default_value);

	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	this->row_groups = parent.row_groups->AddColumn(context, new_column, default_executor);

	// also add this column to client local storage
	local_storage.AddColumn(parent, *this, new_column, default_executor);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : db(parent.db), info(parent.info), is_root(true) {
	// prevent any new tuples from being added to the parent
	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	info->InitializeIndexes(context);

	// first check if there are any indexes that exist that point to the removed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.GetColumnIds()) {
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
	column_definitions.erase_at(removed_column);

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
	local_storage.DropColumn(parent, *this, removed_column);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, BoundConstraint &constraint)
    : db(parent.db), info(parent.info), row_groups(parent.row_groups), is_root(true) {

	// ALTER COLUMN to add a new constraint.

	// Clone the storage info vector or the table.
	for (const auto &index_info : parent.info->index_storage_infos) {
		info->index_storage_infos.push_back(IndexStorageInfo(index_info.name));
	}
	info->InitializeIndexes(context);

	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	if (constraint.type != ConstraintType::UNIQUE) {
		VerifyNewConstraint(local_storage, parent, constraint);
	}
	local_storage.MoveStorage(parent, *this);
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     const vector<StorageIndex> &bound_columns, Expression &cast_expr)
    : db(parent.db), info(parent.info), is_root(true) {
	auto &local_storage = LocalStorage::Get(context, db);
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	info->InitializeIndexes(context);

	// first check if there are any indexes that exist that point to the changed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.GetColumnIds()) {
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
	local_storage.ChangeType(parent, *this, changed_idx, target_type, bound_columns, cast_expr);

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

bool DataTable::IsTemporary() const {
	return info->IsTemporary();
}

AttachedDatabase &DataTable::GetAttached() {
	D_ASSERT(RefersToSameObject(db, info->db));
	return db;
}

const vector<ColumnDefinition> &DataTable::Columns() const {
	return column_definitions;
}

TableIOManager &DataTable::GetTableIOManager() {
	return *info->table_io_manager;
}

TableIOManager &TableIOManager::Get(DataTable &table) {
	return table.GetTableIOManager();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(DuckTransaction &transaction, TableScanState &state,
                               const vector<StorageIndex> &column_ids, TableFilterSet *table_filters) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	auto &local_storage = LocalStorage::Get(transaction);
	state.Initialize(column_ids, table_filters);
	row_groups->InitializeScan(state.table_state, column_ids, table_filters);
	local_storage.InitializeScan(*this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(DuckTransaction &transaction, TableScanState &state,
                                         const vector<StorageIndex> &column_ids, idx_t start_row, idx_t end_row) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	state.Initialize(column_ids);
	row_groups->InitializeScanWithOffset(state.table_state, column_ids, start_row, end_row);
}

idx_t DataTable::GetRowGroupSize() const {
	return row_groups->GetRowGroupSize();
}

idx_t DataTable::MaxThreads(ClientContext &context) const {
	idx_t row_group_size = GetRowGroupSize();
	idx_t parallel_scan_vector_count = row_group_size / STANDARD_VECTOR_SIZE;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
	return GetTotalRows() / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	auto &local_storage = LocalStorage::Get(context, db);
	auto &transaction = DuckTransaction::Get(context, db);
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	row_groups->InitializeParallelScan(state.scan_state);

	local_storage.InitializeParallelScan(*this, state.local_state);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state) {
	if (row_groups->NextParallelScan(context, state.scan_state, scan_state.table_state)) {
		return true;
	}
	auto &local_storage = LocalStorage::Get(context, db);
	if (local_storage.NextParallelScan(context, *this, state.local_state, scan_state.local_state)) {
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state) {
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
// Index Methods
//===--------------------------------------------------------------------===//
shared_ptr<DataTableInfo> &DataTable::GetDataTableInfo() {
	return info;
}

void DataTable::InitializeIndexes(ClientContext &context) {
	info->InitializeIndexes(context);
}

bool DataTable::HasIndexes() const {
	return !info->indexes.Empty();
}

void DataTable::AddIndex(unique_ptr<Index> index) {
	info->indexes.AddIndex(std::move(index));
}

bool DataTable::HasForeignKeyIndex(const vector<PhysicalIndex> &keys, ForeignKeyType type) {
	return info->indexes.FindForeignKeyIndex(keys, type) != nullptr;
}

void DataTable::SetIndexStorageInfo(vector<IndexStorageInfo> index_storage_info) {
	info->index_storage_infos = std::move(index_storage_info);
}

void DataTable::VacuumIndexes() {
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().Vacuum();
		}
		return false;
	});
}

void DataTable::CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count) {
	row_groups->CleanupAppend(lowest_transaction, start, count);
}

bool DataTable::IndexNameIsUnique(const string &name) {
	return info->indexes.NameIsUnique(name);
}

string DataTableInfo::GetSchemaName() {
	return schema;
}

string DataTableInfo::GetTableName() {
	lock_guard<mutex> l(name_lock);
	return table;
}

void DataTableInfo::SetTableName(string name) {
	lock_guard<mutex> l(name_lock);
	table = std::move(name);
}

string DataTable::GetTableName() const {
	return info->GetTableName();
}

void DataTable::SetTableName(string new_name) {
	info->SetTableName(std::move(new_name));
}

TableStorageInfo DataTable::GetStorageInfo() {
	TableStorageInfo result;
	result.cardinality = GetTotalRows();
	info->indexes.Scan([&](Index &index) {
		IndexInfo index_info;
		index_info.is_primary = index.IsPrimary();
		index_info.is_unique = index.IsUnique() || index_info.is_primary;
		index_info.is_foreign = index.IsForeign();
		index_info.column_set = index.GetColumnIdSet();
		result.index_info.push_back(std::move(index_info));
		return false;
	});
	return result;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(DuckTransaction &transaction, DataChunk &result, const vector<StorageIndex> &column_ids,
                      const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	auto lock = info->checkpoint_lock.GetSharedLock();
	row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (!VectorOperations::HasNull(vector, count)) {
		return;
	}

	throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                             Expression &expr, column_t index) {
	auto &col = table.GetColumn(LogicalIndex(index));
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(context, expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (InternalException &ex) {
		throw;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw ConstraintException("Incorrect value for generated column \"%s %s AS (%s)\" : %s", col.Name(),
		                          col.Type().ToString(), col.GeneratedExpression().ToString(), error.RawMessage());
	}
}

static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr,
                                  DataChunk &chunk) {
	ExpressionExecutor executor(context, expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, error.RawMessage());
	} catch (...) {
		// LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	} // LCOV_EXCL_STOP
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(chunk.size(), vdata);

	auto dataptr = UnifiedVectorFormat::GetData<int32_t>(vdata);
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
	if (fk_keys.size() != index.GetColumnIds().size()) {
		return false;
	}
	for (auto &fk_key : fk_keys) {
		bool is_found = false;
		for (auto &index_key : index.GetColumnIds()) {
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

// Find the first index that is not null, and did not find a match
static idx_t FirstMissingMatch(const ManagedSelection &matches) {
	idx_t match_idx = 0;

	for (idx_t i = 0; i < matches.Size(); i++) {
		auto match = matches.IndexMapsToLocation(match_idx, i);
		match_idx += match;
		if (!match) {
			// This index is missing in the matches vector
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t LocateErrorIndex(bool is_append, const ManagedSelection &matches) {
	idx_t failed_index = DConstants::INVALID_INDEX;
	if (!is_append) {
		// We expected to find nothing, so the first error is the first match
		failed_index = matches[0];
	} else {
		// We expected to find matches for all of them, so the first missing match is the first error
		return FirstMissingMatch(matches);
	}
	return failed_index;
}

[[noreturn]] static void ThrowForeignKeyConstraintError(idx_t failed_index, bool is_append, Index &conflict_index,
                                                        DataChunk &input) {
	// The index that caused the conflict has to be bound by this point (or we would not have gotten here)
	D_ASSERT(conflict_index.IsBound());
	auto &index = conflict_index.Cast<BoundIndex>();
	auto verify_type = is_append ? VerifyExistenceType::APPEND_FK : VerifyExistenceType::DELETE_FK;
	D_ASSERT(failed_index != DConstants::INVALID_INDEX);
	auto message = index.GetConstraintViolationMessage(verify_type, failed_index, input);
	throw ConstraintException(message);
}

bool IsForeignKeyConstraintError(bool is_append, idx_t input_count, const ManagedSelection &matches) {
	if (is_append) {
		// We need to find a match for all values
		return matches.Count() != input_count;
	} else {
		// We should not find any matches
		return matches.Count() != 0;
	}
}

static bool IsAppend(VerifyExistenceType verify_type) {
	return verify_type == VerifyExistenceType::APPEND_FK;
}

void DataTable::VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                           DataChunk &chunk, VerifyExistenceType verify_type) {
	const vector<PhysicalIndex> *src_keys_ptr = &bfk.info.fk_keys;
	const vector<PhysicalIndex> *dst_keys_ptr = &bfk.info.pk_keys;

	bool is_append = IsAppend(verify_type);
	if (!is_append) {
		src_keys_ptr = &bfk.info.pk_keys;
		dst_keys_ptr = &bfk.info.fk_keys;
	}

	auto &table_entry_ptr =
	    Catalog::GetEntry<TableCatalogEntry>(context, db.GetName(), bfk.info.schema, bfk.info.table);
	// make the data chunk to check
	vector<LogicalType> types;
	for (auto &col : table_entry_ptr.GetColumns().Physical()) {
		types.emplace_back(col.Type());
	}
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr->size(); i++) {
		dst_chunk.data[(*dst_keys_ptr)[i].index].Reference(chunk.data[(*src_keys_ptr)[i].index]);
	}
	dst_chunk.SetCardinality(chunk.size());
	auto &data_table = table_entry_ptr.GetStorage();

	idx_t count = dst_chunk.size();
	if (count <= 0) {
		return;
	}

	// Set up a way to record conflicts, rather than directly throw on them
	unordered_set<column_t> empty_column_list;
	ConflictInfo empty_conflict_info(empty_column_list, false);
	ConflictManager regular_conflicts(verify_type, count, &empty_conflict_info);
	ConflictManager transaction_conflicts(verify_type, count, &empty_conflict_info);
	regular_conflicts.SetMode(ConflictManagerMode::SCAN);
	transaction_conflicts.SetMode(ConflictManagerMode::SCAN);

	data_table.info->indexes.VerifyForeignKey(*dst_keys_ptr, dst_chunk, regular_conflicts);
	regular_conflicts.Finalize();
	auto &regular_matches = regular_conflicts.Conflicts();

	// check if we can insert the chunk into the reference table's local storage
	auto &local_storage = LocalStorage::Get(context, db);
	bool error = IsForeignKeyConstraintError(is_append, count, regular_matches);
	bool transaction_error = false;
	bool transaction_check = local_storage.Find(data_table);

	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		transact_index.VerifyForeignKey(*dst_keys_ptr, dst_chunk, transaction_conflicts);
		transaction_conflicts.Finalize();
		auto &transaction_matches = transaction_conflicts.Conflicts();
		transaction_error = IsForeignKeyConstraintError(is_append, count, transaction_matches);
	}

	if (!transaction_error && !error) {
		// No error occurred;
		return;
	}

	// Some error occurred, and we likely want to throw
	optional_ptr<Index> index;
	optional_ptr<Index> transaction_index;

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	index = data_table.info->indexes.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		// check whether or not the chunk can be inserted or deleted into the referenced table' storage
		transaction_index = transact_index.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	}

	if (!transaction_check) {
		// Only local state is checked, throw the error
		D_ASSERT(error);
		auto failed_index = LocateErrorIndex(is_append, regular_matches);
		D_ASSERT(failed_index != DConstants::INVALID_INDEX);
		ThrowForeignKeyConstraintError(failed_index, is_append, *index, dst_chunk);
	}
	if (transaction_error && error && is_append) {
		// When we want to do an append, we only throw if the foreign key does not exist in both transaction and local
		// storage
		auto &transaction_matches = transaction_conflicts.Conflicts();
		idx_t failed_index = DConstants::INVALID_INDEX;
		idx_t regular_idx = 0;
		idx_t transaction_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			bool in_regular = regular_matches.IndexMapsToLocation(regular_idx, i);
			regular_idx += in_regular;
			bool in_transaction = transaction_matches.IndexMapsToLocation(transaction_idx, i);
			transaction_idx += in_transaction;

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
		ThrowForeignKeyConstraintError(failed_index, true, *index, dst_chunk);
	}
	if (!is_append) {
		D_ASSERT(transaction_check);
		auto &transaction_matches = transaction_conflicts.Conflicts();
		if (error) {
			auto failed_index = LocateErrorIndex(false, regular_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *index, dst_chunk);
		} else {
			D_ASSERT(transaction_error);
			D_ASSERT(transaction_matches.Count() != DConstants::INVALID_INDEX);
			auto failed_index = LocateErrorIndex(false, transaction_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *transaction_index, dst_chunk);
		}
	}
}

void DataTable::VerifyAppendForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                                 DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, VerifyExistenceType::APPEND_FK);
}

void DataTable::VerifyDeleteForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                                 DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, VerifyExistenceType::DELETE_FK);
}

void DataTable::VerifyNewConstraint(LocalStorage &local_storage, DataTable &parent, const BoundConstraint &constraint) {
	if (constraint.type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}

	parent.row_groups->VerifyNewConstraint(parent, constraint);
	local_storage.VerifyNewConstraint(parent, constraint);
}

bool HasUniqueIndexes(TableIndexList &list) {
	bool has_unique_index = false;
	list.Scan([&](Index &index) {
		if (index.IsUnique()) {
			has_unique_index = true;
			return true;
		}
		return false;
	});
	return has_unique_index;
}

void DataTable::VerifyUniqueIndexes(TableIndexList &indexes, ClientContext &context, DataChunk &chunk,
                                    optional_ptr<ConflictManager> conflict_manager) {
	//! check whether or not the chunk can be inserted into the indexes
	if (!conflict_manager) {
		// Only need to verify that no unique constraints are violated
		indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
				return false;
			}
			D_ASSERT(index.IsBound());
			index.Cast<BoundIndex>().VerifyAppend(chunk);
			return false;
		});
		return;
	}

	D_ASSERT(conflict_manager);
	// The conflict manager is only provided when a ON CONFLICT clause was provided to the INSERT statement

	auto &conflict_info = conflict_manager->GetConflictInfo();
	// First we figure out how many indexes match our conflict target
	// So we can optimize accordingly
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique()) {
			return false;
		}
		if (conflict_info.ConflictTargetMatches(index)) {
			D_ASSERT(index.IsBound());
			conflict_manager->AddIndex(index.Cast<BoundIndex>());
		}
		return false;
	});
	conflict_manager->SetMode(ConflictManagerMode::SCAN);
	// First we verify only the indexes that match our conflict target
	for (auto index : conflict_manager->MatchedIndexes()) {
		index->VerifyAppend(chunk, *conflict_manager);
	}

	conflict_manager->SetMode(ConflictManagerMode::THROW);
	// Then we scan the other indexes, throwing if they cause conflicts on tuples that were not found during
	// the scan
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique()) {
			return false;
		}
		D_ASSERT(index.IsBound());
		auto &bound_index = index.Cast<BoundIndex>();
		if (conflict_manager->MatchedIndex(bound_index)) {
			// Already checked this constraint
			return false;
		}
		bound_index.VerifyAppend(chunk, *conflict_manager);
		return false;
	});
}

void DataTable::VerifyAppendConstraints(ConstraintState &state, ClientContext &context, DataChunk &chunk,
                                        optional_ptr<ConflictManager> conflict_manager) {
	auto &table = state.table;
	if (table.HasGeneratedColumns()) {
		// Verify that the generated columns expression work with the inserted values
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder generated_check_binder(*binder, context, table.name, table.GetColumns(), bound_columns);
		for (auto &col : table.GetColumns().Logical()) {
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

	if (HasUniqueIndexes(info->indexes)) {
		VerifyUniqueIndexes(info->indexes, context, chunk, conflict_manager);
	}

	auto &constraints = table.GetConstraints();
	for (idx_t i = 0; i < state.bound_constraints.size(); i++) {
		auto &base_constraint = constraints[i];
		auto &constraint = state.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
			auto &not_null = base_constraint->Cast<NotNullConstraint>();
			auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));
			VerifyNotNullConstraint(table, chunk.data[bound_not_null.index.index], chunk.size(), col.Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = constraint->Cast<BoundCheckConstraint>();
			VerifyCheckConstraint(context, table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			// These were handled earlier on
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = constraint->Cast<BoundForeignKeyConstraint>();
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

unique_ptr<ConstraintState>
DataTable::InitializeConstraintState(TableCatalogEntry &table,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	return make_uniq<ConstraintState>(table, bound_constraints);
}

void DataTable::InitializeLocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                                      const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeAppend(state, *this);

	state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void DataTable::LocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            bool unsafe) {
	if (chunk.size() == 0) {
		return;
	}
	D_ASSERT(chunk.ColumnCount() == table.GetColumns().PhysicalColumnCount());
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	if (!unsafe) {
		VerifyAppendConstraints(*state.constraint_state, context, chunk);
	}

	// append to the transaction local data
	LocalStorage::Append(state, chunk);
}

void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
	LocalStorage::FinalizeAppend(state);
}

OptimisticDataWriter &DataTable::CreateOptimisticWriter(ClientContext &context) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.CreateOptimisticWriter(*this);
}

void DataTable::FinalizeOptimisticWriter(ClientContext &context, OptimisticDataWriter &writer) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.FinalizeOptimisticWriter(*this, writer);
}

void DataTable::LocalMerge(ClientContext &context, RowGroupCollection &collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.LocalMerge(*this, collection);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);
	storage.LocalAppend(append_state, table, context, chunk);
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);
	for (auto &chunk : collection.Chunks()) {
		storage.LocalAppend(append_state, table, context, chunk);
	}
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::AppendLock(TableAppendState &state) {
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	state.row_start = NumericCast<row_t>(row_groups->GetTotalRows());
	state.current_row = state.row_start;
}

void DataTable::InitializeAppend(DuckTransaction &transaction, TableAppendState &state) {
	// obtain the append lock for this table
	if (!state.append_lock) {
		throw InternalException("DataTable::AppendLock should be called before DataTable::InitializeAppend");
	}
	row_groups->InitializeAppend(transaction, state);
}

void DataTable::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	row_groups->Append(chunk, state);
}

void DataTable::FinalizeAppend(DuckTransaction &transaction, TableAppendState &state) {
	row_groups->FinalizeAppend(transaction, state);
}

void DataTable::ScanTableSegment(DuckTransaction &transaction, idx_t row_start, idx_t count,
                                 const std::function<void(DataChunk &chunk)> &function) {
	if (count == 0) {
		return;
	}
	idx_t end = row_start + count;

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->column_definitions.size(); i++) {
		auto &col = this->column_definitions[i];
		column_ids.emplace_back(i);
		types.push_back(col.Type());
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);

	CreateIndexScanState state;

	InitializeScanWithOffset(transaction, state, column_ids, row_start, row_start + count);
	auto row_start_aligned = state.table_state.row_group->start + state.table_state.vector_index * STANDARD_VECTOR_SIZE;

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

void DataTable::MergeStorage(RowGroupCollection &data, TableIndexList &,
                             optional_ptr<StorageCommitState> commit_state) {
	row_groups->MergeStorage(data, this, commit_state);
	row_groups->Verify();
}

void DataTable::WriteToLog(DuckTransaction &transaction, WriteAheadLog &log, idx_t row_start, idx_t count,
                           optional_ptr<StorageCommitState> commit_state) {
	log.WriteSetTable(info->schema, info->table);
	if (commit_state) {
		idx_t optimistic_count = 0;
		auto entry = commit_state->GetRowGroupData(*this, row_start, optimistic_count);
		if (entry) {
			D_ASSERT(optimistic_count > 0);
			log.WriteRowGroupData(*entry);
			if (optimistic_count > count) {
				throw InternalException(
				    "Optimistically written count cannot exceed actual count (got %llu, but expected count is %llu)",
				    optimistic_count, count);
			}
			// write any remaining (non-optimistically written) rows to the WAL normally
			row_start += optimistic_count;
			count -= optimistic_count;
			if (count == 0) {
				return;
			}
		}
	}
	ScanTableSegment(transaction, row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);
	row_groups->CommitAppend(commit_id, row_start, count);
}

void DataTable::RevertAppendInternal(idx_t start_row) {
	D_ASSERT(is_root);
	// revert appends made to row_groups
	row_groups->RevertAppendInternal(start_row);
}

void DataTable::RevertAppend(DuckTransaction &transaction, idx_t start_row, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	// revert any appends to indexes
	if (!info->indexes.Empty()) {
		idx_t current_row_base = start_row;
		row_t row_data[STANDARD_VECTOR_SIZE];
		Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_data));
		idx_t scan_count = MinValue<idx_t>(count, row_groups->GetTotalRows() - start_row);
		ScanTableSegment(transaction, start_row, scan_count, [&](DataChunk &chunk) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				row_data[i] = NumericCast<row_t>(current_row_base + i);
			}
			info->indexes.Scan([&](Index &index) {
				// We cant add to unbound indexes anyways, so there is no need to revert them
				if (index.IsBound()) {
					index.Cast<BoundIndex>().Delete(chunk, row_identifiers);
				}
				return false;
			});
			current_row_base += chunk.size();
		});
	}

	// we need to vacuum the indexes to remove any buffers that are now empty
	// due to reverting the appends
	info->indexes.Scan([&](Index &index) {
		// We cant add to unbound indexes anyway, so there is no need to vacuum them
		if (index.IsBound()) {
			index.Cast<BoundIndex>().Vacuum();
		}
		return false;
	});

	// revert the data table append
	RevertAppendInternal(start_row);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
ErrorData DataTable::AppendToIndexes(TableIndexList &indexes, DataChunk &chunk, row_t row_start) {
	ErrorData error;
	if (indexes.Empty()) {
		return error;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	vector<BoundIndex *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	indexes.Scan([&](Index &index_to_append) {
		if (!index_to_append.IsBound()) {
			error = ErrorData("Unbound index found in DataTable::AppendToIndexes");
			append_failed = true;
			return true;
		}
		auto &index = index_to_append.Cast<BoundIndex>();
		try {
			error = index.Append(chunk, row_identifiers);
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}
		if (error.HasError()) {
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
	}
	return error;
}

ErrorData DataTable::AppendToIndexes(DataChunk &chunk, row_t row_start) {
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
		if (!index.IsBound()) {
			throw InternalException("Unbound index found in DataTable::RemoveFromIndexes");
		}
		index.Cast<BoundIndex>().Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	D_ASSERT(is_root);
	row_groups->RemoveFromIndexes(info->indexes, row_identifiers, count);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
static bool TableHasDeleteConstraints(TableCatalogEntry &table) {
	for (auto &constraint : table.GetConstraints()) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = constraint->Cast<ForeignKeyConstraint>();
			if (bfk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				return true;
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	return false;
}

void DataTable::VerifyDeleteConstraints(TableDeleteState &state, ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : state.constraint_state->bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = constraint->Cast<BoundForeignKeyConstraint>();
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

unique_ptr<TableDeleteState> DataTable::InitializeDelete(TableCatalogEntry &table, ClientContext &context,
                                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// initialize indexes (if any)
	info->InitializeIndexes(context);

	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> types;
	auto result = make_uniq<TableDeleteState>();
	result->has_delete_constraints = TableHasDeleteConstraints(table);
	if (result->has_delete_constraints) {
		// initialize the chunk if there are any constraints to verify
		for (idx_t i = 0; i < column_definitions.size(); i++) {
			result->col_ids.emplace_back(column_definitions[i].StorageOid());
			types.emplace_back(column_definitions[i].Type());
		}
		result->verify_chunk.Initialize(Allocator::Get(context), types);
		result->constraint_state = make_uniq<ConstraintState>(table, bound_constraints);
	}
	return result;
}

idx_t DataTable::Delete(TableDeleteState &state, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return 0;
	}

	auto &transaction = DuckTransaction::Get(context, db);
	auto &local_storage = LocalStorage::Get(transaction);

	row_identifiers.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);

	idx_t pos = 0;
	idx_t delete_count = 0;
	while (pos < count) {
		idx_t start = pos;
		bool is_transaction_delete = ids[pos] >= MAX_ROW_ID;
		// figure out which batch of rows to delete now
		for (pos++; pos < count; pos++) {
			bool row_is_transaction_delete = ids[pos] >= MAX_ROW_ID;
			if (row_is_transaction_delete != is_transaction_delete) {
				break;
			}
		}
		idx_t current_offset = start;
		idx_t current_count = pos - start;

		Vector offset_ids(row_identifiers, current_offset, pos);
		if (is_transaction_delete) {
			// transaction-local delete
			if (state.has_delete_constraints) {
				// perform the constraint verification
				ColumnFetchState fetch_state;
				local_storage.FetchChunk(*this, offset_ids, current_count, state.col_ids, state.verify_chunk,
				                         fetch_state);
				VerifyDeleteConstraints(state, context, state.verify_chunk);
			}
			delete_count += local_storage.Delete(*this, offset_ids, current_count);
		} else {
			// regular table delete
			if (state.has_delete_constraints) {
				// perform the constraint verification
				ColumnFetchState fetch_state;
				Fetch(transaction, state.verify_chunk, state.col_ids, offset_ids, current_count, fetch_state);
				VerifyDeleteConstraints(state, context, state.verify_chunk);
			}
			delete_count += row_groups->Delete(transaction, *this, ids + current_offset, current_count);
		}
	}
	return delete_count;
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

void DataTable::VerifyUpdateConstraints(ConstraintState &state, ClientContext &context, DataChunk &chunk,
                                        const vector<PhysicalIndex> &column_ids) {
	auto &table = state.table;
	auto &constraints = table.GetConstraints();
	auto &bound_constraints = state.bound_constraints;
	for (idx_t constr_idx = 0; constr_idx < bound_constraints.size(); constr_idx++) {
		auto &base_constraint = constraints[constr_idx];
		auto &constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
			auto &not_null = base_constraint->Cast<NotNullConstraint>();
			// check if the constraint is in the list of column_ids
			for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				if (column_ids[col_idx] == bound_not_null.index) {
					// found the column id: check the data in
					auto &col = table.GetColumn(LogicalIndex(not_null.index));
					VerifyNotNullConstraint(table, chunk.data[col_idx], chunk.size(), col.Name());
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = constraint->Cast<BoundCheckConstraint>();

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
		D_ASSERT(index.IsBound());
		D_ASSERT(!index.Cast<BoundIndex>().IndexIsUpdated(column_ids));
		return false;
	});

#endif
}

unique_ptr<TableUpdateState> DataTable::InitializeUpdate(TableCatalogEntry &table, ClientContext &context,
                                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// check that there are no unknown indexes
	info->InitializeIndexes(context);

	auto result = make_uniq<TableUpdateState>();
	result->constraint_state = InitializeConstraintState(table, bound_constraints);
	return result;
}

void DataTable::Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
                       const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(column_ids.size() == updates.ColumnCount());
	updates.Verify();

	auto count = updates.size();
	if (count == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(*state.constraint_state, context, updates, column_ids);

	// now perform the actual update
	Vector max_row_id_vec(Value::BIGINT(MAX_ROW_ID));
	Vector row_ids_slice(LogicalType::BIGINT);
	DataChunk updates_slice;
	updates_slice.InitializeEmpty(updates.GetTypes());

	SelectionVector sel_local_update(count), sel_global_update(count);
	auto n_local_update = VectorOperations::GreaterThanEquals(row_ids, max_row_id_vec, nullptr, count,
	                                                          &sel_local_update, &sel_global_update);
	auto n_global_update = count - n_local_update;

	// row id > MAX_ROW_ID? transaction-local storage
	if (n_local_update > 0) {
		updates_slice.Slice(updates, sel_local_update, n_local_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_local_update, n_local_update);
		row_ids_slice.Flatten(n_local_update);

		LocalStorage::Get(context, db).Update(*this, row_ids_slice, column_ids, updates_slice);
	}

	// otherwise global storage
	if (n_global_update > 0) {
		auto &transaction = DuckTransaction::Get(context, db);
		updates_slice.Slice(updates, sel_global_update, n_global_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_global_update, n_global_update);
		row_ids_slice.Flatten(n_global_update);

		transaction.UpdateCollection(row_groups);
		row_groups->Update(transaction, FlatVector::GetData<row_t>(row_ids_slice), column_ids, updates_slice);
	}
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
	auto &transaction = DuckTransaction::Get(context, db);

	updates.Flatten();
	row_ids.Flatten(updates.size());
	row_groups->UpdateColumn(transaction, row_ids, column_path, updates);
}

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//
unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return row_groups->CopyStats(column_id);
}

void DataTable::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	row_groups->SetDistinct(column_id, std::move(distinct_stats));
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
unique_ptr<StorageLockKey> DataTable::GetSharedCheckpointLock() {
	return info->checkpoint_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DataTable::GetCheckpointLock() {
	return info->checkpoint_lock.GetExclusiveLock();
}

void DataTable::Checkpoint(TableDataWriter &writer, Serializer &serializer) {
	// checkpoint each individual row group
	TableStatistics global_stats;
	row_groups->CopyStats(global_stats);
	row_groups->Checkpoint(writer, global_stats);

	// The row group payload data has been written. Now write:
	//   column stats
	//   row-group pointers
	//   table pointer
	//   index data
	writer.FinalizeTable(global_stats, info.get(), serializer);
}

void DataTable::CommitDropColumn(idx_t index) {
	row_groups->CommitDropColumn(index);
}

idx_t DataTable::ColumnCount() const {
	return column_definitions.size();
}

idx_t DataTable::GetTotalRows() const {
	return row_groups->GetTotalRows();
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified, so they can be reclaimed later on
	row_groups->CommitDropTable();

	// propagate dropping this table to its indexes: frees all index memory
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		index.Cast<BoundIndex>().CommitDrop();
		return false;
	});
}

//===--------------------------------------------------------------------===//
// Column Segment Info
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> DataTable::GetColumnSegmentInfo() {
	auto lock = GetSharedCheckpointLock();
	return row_groups->GetColumnSegmentInfo();
}

//===--------------------------------------------------------------------===//
// Index Constraint Creation
//===--------------------------------------------------------------------===//
void DataTable::AddIndex(const ColumnList &columns, const vector<LogicalIndex> &column_indexes,
                         const IndexConstraintType type, const IndexStorageInfo &index_info) {
	if (!IsRoot()) {
		throw TransactionException("cannot add an index to a table that has been altered!");
	}

	// Fetch the column types and create bound column reference expressions.
	vector<column_t> physical_ids;
	vector<unique_ptr<Expression>> expressions;

	for (const auto column_index : column_indexes) {
		auto binding = ColumnBinding(0, physical_ids.size());
		auto &col = columns.GetColumn(column_index);
		auto ref = make_uniq<BoundColumnRefExpression>(col.Name(), col.Type(), binding);
		expressions.push_back(std::move(ref));
		physical_ids.push_back(col.Physical().index);
	}

	// Create an ART around the expressions.
	auto &io_manager = TableIOManager::Get(*this);
	auto art = make_uniq<ART>(index_info.name, type, physical_ids, io_manager, std::move(expressions), db, nullptr,
	                          index_info);
	info->indexes.AddIndex(std::move(art));
}

} // namespace duckdb
