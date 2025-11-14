#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

DataTableInfo::DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, string schema,
                             string table)
    : db(db), table_io_manager(std::move(table_io_manager_p)), schema(std::move(schema)), table(std::move(table)) {
}

void DataTableInfo::BindIndexes(ClientContext &context, const char *index_type) {
	indexes.Bind(context, *this, index_type);
}

bool DataTableInfo::IsTemporary() const {
	return db.IsTemporary();
}

DataTable::DataTable(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, const string &schema,
                     const string &table, vector<ColumnDefinition> column_definitions_p,
                     unique_ptr<PersistentTableData> data)
    : db(db), info(make_shared_ptr<DataTableInfo>(db, std::move(table_io_manager_p), schema, table)),
      column_definitions(std::move(column_definitions_p)), version(DataTableVersion::MAIN_TABLE) {
	// initialize the table with the existing data from disk, if any
	auto types = GetTypes();
	auto &io_manager = TableIOManager::Get(*this);
	this->row_groups = make_shared_ptr<RowGroupCollection>(info, io_manager, types, 0);
	if (data && data->row_group_count > 0) {
		this->row_groups->Initialize(*data);
		if (!HasIndexes()) {
			// if we don't have indexes, always append a new row group upon appending
			// we can clean up this row group again when vacuuming
			// since we don't yet support vacuum when there are indexes, we only do this when there are no indexes
			row_groups->SetAppendRequiresNewRowGroup();
		}
	} else {
		this->row_groups->InitializeEmpty();
		D_ASSERT(row_groups->GetTotalRows() == 0);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression &default_value)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
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
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
	// prevent any new tuples from being added to the parent
	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	// Bind all indexes.
	info->BindIndexes(context);

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
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, BoundConstraint &constraint)
    : db(parent.db), info(parent.info), row_groups(parent.row_groups), version(DataTableVersion::MAIN_TABLE) {
	// ALTER COLUMN to add a new constraint.

	// Clone the storage info vector or the table.
	for (const auto &index_info : parent.info->index_storage_infos) {
		info->index_storage_infos.push_back(IndexStorageInfo(index_info.name));
	}

	// Bind all indexes.
	info->BindIndexes(context);

	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	if (constraint.type != ConstraintType::UNIQUE) {
		VerifyNewConstraint(local_storage, parent, constraint);
	}
	local_storage.MoveStorage(parent, *this);
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     const vector<StorageIndex> &bound_columns, Expression &cast_expr)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
	auto &local_storage = LocalStorage::Get(context, db);
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	// Bind all indexes.
	info->BindIndexes(context);

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
	row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);

	// scan the original table, and fill the new column with the transformed value
	local_storage.ChangeType(parent, *this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.version = DataTableVersion::ALTERED;
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
void DataTable::InitializeScan(ClientContext &context, DuckTransaction &transaction, TableScanState &state,
                               const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> table_filters) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	auto &local_storage = LocalStorage::Get(transaction);
	state.Initialize(column_ids, context, table_filters);
	row_groups->InitializeScan(context, state.table_state, column_ids, table_filters);
	local_storage.InitializeScan(*this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(DuckTransaction &transaction, TableScanState &state,
                                         const vector<StorageIndex> &column_ids, idx_t start_row, idx_t end_row) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	state.Initialize(column_ids);
	row_groups->InitializeScanWithOffset(QueryContext(), state.table_state, column_ids, start_row, end_row);
}

idx_t DataTable::GetRowGroupSize() const {
	return row_groups->GetRowGroupSize();
}

vector<PartitionStatistics> DataTable::GetPartitionStats(ClientContext &context) {
	auto result = row_groups->GetPartitionStats();
	auto &local_storage = LocalStorage::Get(context, db);
	auto local_partitions = local_storage.GetPartitionStats(*this);
	result.insert(result.end(), local_partitions.begin(), local_partitions.end());
	return result;
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

void DataTable::BindIndexes(ClientContext &context) {
	info->BindIndexes(context);
}

bool DataTable::HasIndexes() const {
	return !info->indexes.Empty();
}

bool DataTable::HasUniqueIndexes() const {
	if (!HasIndexes()) {
		return false;
	}
	bool has_unique_index = false;
	info->indexes.Scan([&](Index &index) {
		if (index.IsUnique()) {
			has_unique_index = true;
			return true;
		}
		return false;
	});
	return has_unique_index;
}

void DataTable::AddIndex(unique_ptr<Index> index) {
	info->indexes.AddIndex(std::move(index));
}

bool DataTable::HasForeignKeyIndex(const vector<PhysicalIndex> &keys, ForeignKeyType type) {
	auto index = info->indexes.FindForeignKeyIndex(keys, type);
	return index != nullptr;
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

void DataTable::VerifyIndexBuffers() {
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().VerifyBuffers();
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
	auto lock = transaction.SharedLockTable(*info);
	row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state);
}

bool DataTable::CanFetch(DuckTransaction &transaction, const row_t row_id) {
	auto lock = transaction.SharedLockTable(*info);
	return row_groups->CanFetch(transaction, row_id);
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

static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr, DataChunk &chunk,
                                  CheckConstraint &check) {
	ExpressionExecutor executor(context, expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw ConstraintException("CHECK constraint failed on table %s with expression %s (Error: %s)", table.name,
		                          check.ToString(), error.RawMessage());
	} catch (...) {
		// LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed on table %s with expression %s (Unknown Error)", table.name,
		                          check.ToString());
	} // LCOV_EXCL_STOP
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(chunk.size(), vdata);

	auto dataptr = UnifiedVectorFormat::GetData<int32_t>(vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed on table %s with expression %s", table.name,
			                          check.ToString());
		}
	}
}

static idx_t FirstMissingMatch(ConflictManager &manager, const idx_t count) {
	// Find the first non-NULL index without a match.
	if (!manager.HasConflicts()) {
		return 0;
	}
	auto conflict = manager.GetFirstInvalidIndex(count, true);
	if (conflict.IsValid()) {
		return conflict.GetIndex();
	}
	throw InternalException("expected a missing match for the index");
}

idx_t LocateErrorIndex(ConflictManager &manager, const bool is_append, const idx_t count) {
	// We expected to find nothing, so the first error is the first match.
	if (!is_append) {
		return manager.GetFirstIndex();
	}
	// We expected to find matches for each row.
	// Thus, the first missing match is the first error.
	return FirstMissingMatch(manager, count);
}

static string ConstructForeignKeyError(optional_idx conflict, bool is_append, Index &index, DataChunk &input) {
	D_ASSERT(index.IsBound());
	auto &bound_index = index.Cast<BoundIndex>();
	auto verify_type = is_append ? VerifyExistenceType::APPEND_FK : VerifyExistenceType::DELETE_FK;
	return bound_index.GetConstraintViolationMessage(verify_type, conflict.GetIndex(), input);
}

bool IsForeignKeyConstraintError(const ConflictManager &manager, const bool is_append, const idx_t input_count) {
	if (is_append) {
		// We need to find a match for all values.
		return manager.ConflictCount() != input_count;
	}
	// Nothing should match.
	return manager.ConflictCount() != 0;
}

static bool IsAppend(VerifyExistenceType verify_type) {
	return verify_type == VerifyExistenceType::APPEND_FK;
}

void DataTable::VerifyForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                           const BoundForeignKeyConstraint &bound_foreign_key, ClientContext &context,
                                           DataChunk &chunk, VerifyExistenceType verify_type) {
	reference<const vector<PhysicalIndex>> src_keys_ptr = bound_foreign_key.info.fk_keys;
	reference<const vector<PhysicalIndex>> dst_keys_ptr = bound_foreign_key.info.pk_keys;

	auto is_append = IsAppend(verify_type);
	if (!is_append) {
		src_keys_ptr = bound_foreign_key.info.pk_keys;
		dst_keys_ptr = bound_foreign_key.info.fk_keys;
	}

	// Get the column types in their physical order.
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, db.GetName(), bound_foreign_key.info.schema,
	                                                         bound_foreign_key.info.table);
	vector<LogicalType> types;
	for (auto &col : table_entry.GetColumns().Physical()) {
		types.emplace_back(col.Type());
	}

	// Create the data chunk that has to be verified.
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr.get().size(); i++) {
		auto &src_chunk = chunk.data[src_keys_ptr.get()[i].index];
		dst_chunk.data[dst_keys_ptr.get()[i].index].Reference(src_chunk);
	}

	auto count = chunk.size();
	dst_chunk.SetCardinality(count);
	if (count <= 0) {
		return;
	}

	// Record conflicts instead of throwing immediately.
	unordered_set<column_t> empty_column_list;
	ConflictInfo empty_conflict_info(empty_column_list, false);

	ConflictManager global_conflict_manager(verify_type, count, &empty_conflict_info);
	global_conflict_manager.SetMode(ConflictManagerMode::SCAN);
	ConflictManager local_conflict_manager(verify_type, count, &empty_conflict_info);
	local_conflict_manager.SetMode(ConflictManagerMode::SCAN);

	// Global constraint verification.
	auto &data_table = table_entry.GetStorage();
	data_table.info->indexes.VerifyForeignKey(storage, dst_keys_ptr, dst_chunk, global_conflict_manager);

	// Check if we can insert the chunk into the local storage.
	auto &local_storage = LocalStorage::Get(context, db);
	bool local_error = false;
	auto local_verification = local_storage.Find(data_table);

	// Local constraint verification.
	if (local_verification) {
		auto &local_indexes = local_storage.GetIndexes(context, data_table);
		local_indexes.VerifyForeignKey(storage, dst_keys_ptr, dst_chunk, local_conflict_manager);
		local_error = IsForeignKeyConstraintError(local_conflict_manager, is_append, count);
	}
	// Global constraint verification.
	auto global_error = IsForeignKeyConstraintError(global_conflict_manager, is_append, count);

	// No constraint violation.
	if (!global_error && !local_error) {
		return;
	}

	// Either a global or local error occurred.
	// We construct the error message and throw.
	optional_ptr<Index> index;
	optional_ptr<Index> transaction_index;
	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// Check whether we can insert into the foreign key table, or delete from the reference table.
	index = data_table.info->indexes.FindForeignKeyIndex(dst_keys_ptr, fk_type);
	if (!local_verification) {
		auto conflict = LocateErrorIndex(global_conflict_manager, is_append, count);
		auto message = ConstructForeignKeyError(conflict, is_append, *index, dst_chunk);
		throw ConstraintException(message);
	}

	auto &transact_index = local_storage.GetIndexes(context, data_table);
	transaction_index = transact_index.FindForeignKeyIndex(dst_keys_ptr, fk_type);

	if (local_error && global_error && is_append) {
		// For appends, we throw if the foreign key neither exists in the transaction nor the local storage.
		optional_idx conflict;
		auto global_conflicts = global_conflict_manager.HasConflicts();
		auto local_conflicts = local_conflict_manager.HasConflicts();
		if (!global_conflicts && !local_conflicts) {
			conflict = 0;
		} else if (!global_conflicts && local_conflicts) {
			conflict = local_conflict_manager.GetFirstInvalidIndex(count);
		} else if (global_conflicts && !local_conflicts) {
			conflict = global_conflict_manager.GetFirstInvalidIndex(count);
		} else {
			auto &global_validity = global_conflict_manager.GetFirstValidity();
			auto &local_validity = local_conflict_manager.GetFirstValidity();
			for (idx_t i = 0; i < count; i++) {
				auto global_match = global_validity.RowIsValid(i);
				auto local_match = local_validity.RowIsValid(i);
				if (!global_match && !local_match) {
					conflict = i;
					break;
				}
			}
		}

		if (!conflict.IsValid()) {
			// We don't throw, every value was present in either regular or transaction storage
			return;
		}
		auto message = ConstructForeignKeyError(conflict, true, *index, dst_chunk);
		throw ConstraintException(message);
	}

	if (!is_append) {
		if (global_error) {
			auto conflict = LocateErrorIndex(global_conflict_manager, false, count);
			auto message = ConstructForeignKeyError(conflict, false, *index, dst_chunk);
			throw ConstraintException(message);
		}

		D_ASSERT(local_conflict_manager.HasConflicts());
		auto conflict = LocateErrorIndex(local_conflict_manager, false, count);
		auto message = ConstructForeignKeyError(conflict, false, *transaction_index, dst_chunk);
		throw ConstraintException(message);
	}
}

void DataTable::VerifyAppendForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                                 const BoundForeignKeyConstraint &bound_foreign_key,
                                                 ClientContext &context, DataChunk &chunk) {
	VerifyForeignKeyConstraint(storage, bound_foreign_key, context, chunk, VerifyExistenceType::APPEND_FK);
}

void DataTable::VerifyDeleteForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                                 const BoundForeignKeyConstraint &bound_foreign_key,
                                                 ClientContext &context, DataChunk &chunk) {
	VerifyForeignKeyConstraint(storage, bound_foreign_key, context, chunk, VerifyExistenceType::DELETE_FK);
}

void DataTable::VerifyNewConstraint(LocalStorage &local_storage, DataTable &parent, const BoundConstraint &constraint) {
	if (constraint.type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}

	parent.row_groups->VerifyNewConstraint(local_storage.GetClientContext(), parent, constraint);
	local_storage.VerifyNewConstraint(parent, constraint);
}

void DataTable::VerifyUniqueIndexes(TableIndexList &indexes, optional_ptr<LocalTableStorage> storage, DataChunk &chunk,
                                    optional_ptr<ConflictManager> manager) {
	// Verify the constraint without a conflict manager.
	if (!manager) {
		return indexes.Scan([&](Index &index) {
			if (!index.IsUnique() || index.GetIndexType() != ART::TYPE_NAME) {
				return false;
			}
			D_ASSERT(index.IsBound());
			auto &art = index.Cast<ART>();
			if (storage) {
				auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
				D_ASSERT(!delete_index || delete_index->IsBound());
				IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, delete_index);
				art.VerifyAppend(chunk, index_append_info, nullptr);
			} else {
				IndexAppendInfo index_append_info;
				art.VerifyAppend(chunk, index_append_info, nullptr);
			}
			return false;
		});
	}

	// The conflict manager is only provided for statements containing ON CONFLICT.
	auto &conflict_info = manager->GetConflictInfo();

	// Find all indexes matching the conflict target.
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique() || index.GetIndexType() != ART::TYPE_NAME) {
			return false;
		}
		if (!conflict_info.ConflictTargetMatches(index)) {
			return false;
		}
		D_ASSERT(index.IsBound());
		auto &art = index.Cast<ART>();
		if (storage) {
			auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
			D_ASSERT(!delete_index || delete_index->IsBound());
			manager->AddIndex(art, delete_index);
		} else {
			manager->AddIndex(art, nullptr);
		}
		return false;
	});

	// Verify indexes matching the conflict target.
	manager->SetMode(ConflictManagerMode::SCAN);
	auto &matching_indexes = manager->MatchingIndexes();
	auto &matching_delete_indexes = manager->MatchingDeleteIndexes();
	IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, nullptr);
	for (idx_t i = 0; i < matching_indexes.size(); i++) {
		index_append_info.delete_index = matching_delete_indexes[i];
		matching_indexes[i].get().VerifyAppend(chunk, index_append_info, *manager);
	}

	// Scan the other indexes and throw, if there are any conflicts.
	manager->SetMode(ConflictManagerMode::THROW);
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique() || index.GetIndexType() != ART::TYPE_NAME) {
			return false;
		}
		if (manager->IndexMatches(index.Cast<BoundIndex>())) {
			return false;
		}
		D_ASSERT(index.IsBound());
		auto &art = index.Cast<ART>();
		if (storage) {
			auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
			D_ASSERT(!delete_index || delete_index->IsBound());
			IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, delete_index);
			art.VerifyAppend(chunk, index_append_info, *manager);
		} else {
			IndexAppendInfo index_append_info;
			art.VerifyAppend(chunk, index_append_info, *manager);
		}
		return false;
	});
}

void DataTable::VerifyAppendConstraints(ConstraintState &constraint_state, ClientContext &context, DataChunk &chunk,
                                        optional_ptr<LocalTableStorage> storage,
                                        optional_ptr<ConflictManager> manager) {
	auto &table = constraint_state.table;
	if (table.HasGeneratedColumns()) {
		// Verify the generated columns against the inserted values.
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

	if (HasUniqueIndexes()) {
		VerifyUniqueIndexes(info->indexes, storage, chunk, manager);
	}

	auto &constraints = table.GetConstraints();
	for (idx_t i = 0; i < constraint_state.bound_constraints.size(); i++) {
		auto &base_constraint = constraints[i];
		auto &constraint = constraint_state.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
			auto &not_null = base_constraint->Cast<NotNullConstraint>();
			auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));
			VerifyNotNullConstraint(table, chunk.data[bound_not_null.index.index], chunk.size(), col.Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = base_constraint->Cast<CheckConstraint>();
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();
			VerifyCheckConstraint(context, table, *bound_check.expression, chunk, check);
			break;
		}
		case ConstraintType::UNIQUE: {
			// These were handled earlier.
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bound_foreign_key = constraint->Cast<BoundForeignKeyConstraint>();
			if (bound_foreign_key.info.IsAppendConstraint()) {
				VerifyAppendForeignKeyConstraint(storage, bound_foreign_key, context, chunk);
			}
			break;
		}
		default:
			throw InternalException("invalid constraint type");
		}
	}
}

unique_ptr<ConstraintState>
DataTable::InitializeConstraintState(TableCatalogEntry &table,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	return make_uniq<ConstraintState>(table, bound_constraints);
}

string DataTable::TableModification() const {
	switch (version.load()) {
	case DataTableVersion::MAIN_TABLE:
		return "no changes";
	case DataTableVersion::ALTERED:
		return "altered";
	case DataTableVersion::DROPPED:
		return "dropped";
	default:
		throw InternalException("Unrecognized table version");
	}
}

void DataTable::InitializeLocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                                      const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeAppend(state, *this);
	state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void DataTable::InitializeLocalStorage(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                                       const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}

	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeStorage(state, *this);
	state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void DataTable::LocalAppend(LocalAppendState &state, ClientContext &context, DataChunk &chunk, bool unsafe) {
	if (chunk.size() == 0) {
		return;
	}
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}
	chunk.Verify();

	// Insert any row ids into the DELETE ART and verify constraints afterward.
	// This happens only for the global indexes.
	if (!unsafe) {
		auto &constraint_state = *state.constraint_state;
		VerifyAppendConstraints(constraint_state, context, chunk, *state.storage, nullptr);
	}

	// Append to the transaction-local data.
	auto data_table_info = GetDataTableInfo();
	LocalStorage::Append(state, chunk, *data_table_info);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	InitializeLocalAppend(append_state, table, context, bound_constraints);
	LocalAppend(append_state, context, chunk, false);
	FinalizeLocalAppend(append_state);
}

void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
	LocalStorage::FinalizeAppend(state);
}

PhysicalIndex DataTable::CreateOptimisticCollection(ClientContext &context,
                                                    unique_ptr<OptimisticWriteCollection> collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.CreateOptimisticCollection(*this, std::move(collection));
}

OptimisticWriteCollection &DataTable::GetOptimisticCollection(ClientContext &context,
                                                              const PhysicalIndex collection_index) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.GetOptimisticCollection(*this, collection_index);
}

void DataTable::ResetOptimisticCollection(ClientContext &context, const PhysicalIndex collection_index) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.ResetOptimisticCollection(*this, collection_index);
}

OptimisticDataWriter &DataTable::GetOptimisticWriter(ClientContext &context) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.GetOptimisticWriter(*this);
}

void DataTable::LocalMerge(ClientContext &context, OptimisticWriteCollection &collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.LocalMerge(*this, collection);
}

void DataTable::LocalWALAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                               const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);

	storage.LocalAppend(append_state, context, chunk, true);
	append_state.storage->index_append_mode = IndexAppendMode::INSERT_DUPLICATES;
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints, Vector &row_ids,
                            DataChunk &delete_chunk) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);
	append_state.storage->AppendToDeleteIndexes(row_ids, delete_chunk);

	storage.LocalAppend(append_state, context, chunk, false);
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                            optional_ptr<const vector<LogicalIndex>> column_ids) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);

	if (!column_ids || column_ids->empty()) {
		for (auto &chunk : collection.Chunks()) {
			storage.LocalAppend(append_state, context, chunk, false);
		}
		storage.FinalizeLocalAppend(append_state);
		return;
	}

	auto &column_list = table.GetColumns();
	map<PhysicalIndex, unique_ptr<Expression>> active_expressions;
	for (idx_t i = 0; i < column_ids->size(); i++) {
		auto &col = column_list.GetColumn((*column_ids)[i]);
		auto expr = make_uniq<BoundReferenceExpression>(col.Name(), col.Type(), i);
		active_expressions[col.Physical()] = std::move(expr);
	}

	auto binder = Binder::CreateBinder(context);
	ConstantBinder default_binder(*binder, context, "DEFAULT value");
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < column_list.PhysicalColumnCount(); i++) {
		auto expr = active_expressions.find(PhysicalIndex(i));
		if (expr != active_expressions.end()) {
			expressions.push_back(std::move(expr->second));
			continue;
		}

		auto &col = column_list.GetColumn(PhysicalIndex(i));
		if (!col.HasDefaultValue()) {
			auto null_expr = make_uniq<BoundConstantExpression>(Value(col.Type()));
			expressions.push_back(std::move(null_expr));
			continue;
		}

		auto default_copy = col.DefaultValue().Copy();
		default_binder.target_type = col.Type();
		auto bound_default = default_binder.Bind(default_copy);
		expressions.push_back(std::move(bound_default));
	}

	ExpressionExecutor expression_executor(context, expressions);
	DataChunk result;
	result.Initialize(context, table.GetTypes());

	for (auto &chunk : collection.Chunks()) {
		expression_executor.Execute(chunk, result);
		storage.LocalAppend(append_state, context, result, false);
		result.Reset();
	}
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::AppendLock(TableAppendState &state) {
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
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
	D_ASSERT(IsMainTable());
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
	auto row_start_aligned =
	    state.table_state.row_group->row_start + state.table_state.vector_index * STANDARD_VECTOR_SIZE;

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
	D_ASSERT(IsMainTable());
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
				// We cannot add to unbound indexes, so there is no need to revert them.
				if (index.IsBound()) {
					index.Cast<BoundIndex>().Delete(chunk, row_identifiers);
				}
				return false;
			});
			current_row_base += chunk.size();
		});
	}

#ifdef DEBUG
	// Verify that our index memory is stable.
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().VerifyBuffers();
		}
		return false;
	});
#endif

	// revert the data table append
	RevertAppendInternal(start_row);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
ErrorData DataTable::AppendToIndexes(TableIndexList &indexes, optional_ptr<TableIndexList> delete_indexes,
                                     DataChunk &table_chunk, DataChunk &index_chunk,
                                     const vector<StorageIndex> &mapped_column_ids, row_t row_start,
                                     const IndexAppendMode index_append_mode) {
	// Generate the vector of row identifiers.
	Vector row_ids(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_ids, table_chunk.size(), row_start, 1);

	vector<reference<BoundIndex>> already_appended;
	bool append_failed = false;

	// Append the entries to the indexes.
	ErrorData error;
	indexes.Scan([&](Index &index) {
		if (!index.IsBound()) {
			// Buffer only the key columns, and store their mapping.
			auto &unbound_index = index.Cast<UnboundIndex>();
			unbound_index.BufferChunk(index_chunk, row_ids, mapped_column_ids, BufferedIndexReplay::INSERT_ENTRY);
			return false;
		}

		auto &bound_index = index.Cast<BoundIndex>();

		// Find the matching delete index.
		optional_ptr<BoundIndex> delete_index;
		if (bound_index.IsUnique()) {
			if (delete_indexes) {
				delete_index = delete_indexes->Find(bound_index.name);
			}
		}

		try {
			// Append the mock chunk containing empty columns for non-key columns.
			IndexAppendInfo index_append_info(index_append_mode, delete_index);
			error = bound_index.Append(table_chunk, row_ids, index_append_info);
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}

		if (error.HasError()) {
			append_failed = true;
			return true;
		}

		already_appended.push_back(bound_index);
		return false;
	});

	if (append_failed) {
		// Constraint violation: remove any appended entries from previous indexes (if any).
		for (auto index : already_appended) {
			index.get().Delete(table_chunk, row_ids);
		}
	}
	return error;
}

ErrorData DataTable::AppendToIndexes(optional_ptr<TableIndexList> delete_indexes, DataChunk &table_chunk,
                                     DataChunk &index_chunk, const vector<StorageIndex> &mapped_column_ids,
                                     row_t row_start, const IndexAppendMode index_append_mode) {
	D_ASSERT(IsMainTable());
	return AppendToIndexes(info->indexes, delete_indexes, table_chunk, index_chunk, mapped_column_ids, row_start,
	                       index_append_mode);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(IsMainTable());
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
	D_ASSERT(IsMainTable());
	info->indexes.Scan([&](Index &index) {
		if (!index.IsBound()) {
			throw InternalException("Unbound index found in DataTable::RemoveFromIndexes");
		}
		auto &bound_index = index.Cast<BoundIndex>();
		bound_index.Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(const QueryContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(IsMainTable());
	row_groups->RemoveFromIndexes(context, info->indexes, row_identifiers, count);
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
			auto &foreign_key = constraint->Cast<ForeignKeyConstraint>();
			if (foreign_key.info.IsDeleteConstraint()) {
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

void DataTable::VerifyDeleteConstraints(optional_ptr<LocalTableStorage> storage, TableDeleteState &state,
                                        ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : state.constraint_state->bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bound_foreign_key = constraint->Cast<BoundForeignKeyConstraint>();
			if (bound_foreign_key.info.IsDeleteConstraint()) {
				VerifyDeleteForeignKeyConstraint(storage, bound_foreign_key, context, chunk);
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
	// Bind all indexes.
	info->BindIndexes(context);

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
	auto storage = local_storage.GetStorage(*this);

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

		// This is a transaction-local DELETE.
		if (is_transaction_delete) {
			if (state.has_delete_constraints) {
				// Verify any delete constraints.
				ColumnFetchState fetch_state;
				local_storage.FetchChunk(*this, offset_ids, current_count, state.col_ids, state.verify_chunk,
				                         fetch_state);
				VerifyDeleteConstraints(storage, state, context, state.verify_chunk);
			}
			delete_count += local_storage.Delete(*this, offset_ids, current_count);
			continue;
		}

		// This is a regular DELETE.
		if (state.has_delete_constraints) {
			// Verify any delete constraints.
			ColumnFetchState fetch_state;
			Fetch(transaction, state.verify_chunk, state.col_ids, offset_ids, current_count, fetch_state);
			VerifyDeleteConstraints(storage, state, context, state.verify_chunk);
		}
		delete_count += row_groups->Delete(transaction, *this, ids + current_offset, current_count);
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
			auto &check = base_constraint->Cast<CheckConstraint>();
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, bound_check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(context, table, *bound_check.expression, mock_chunk, check);
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

#ifdef DEBUG
	// Ensure that we never call UPDATE for indexed columns.
	// Instead, we must rewrite these updates into DELETE + INSERT.
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		D_ASSERT(!index.Cast<BoundIndex>().IndexIsUpdated(column_ids));
		return false;
	});
#endif
}

unique_ptr<TableUpdateState> DataTable::InitializeUpdate(TableCatalogEntry &table, ClientContext &context,
                                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// Bind all indexes.
	info->BindIndexes(context);
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

	if (!IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: attempting to update table \"%s\" but it has been %s by a different transaction",
		    GetTableName(), TableModification());
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
		transaction.ModifyTable(*this);
		updates_slice.Slice(updates, sel_global_update, n_global_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_global_update, n_global_update);
		row_ids_slice.Flatten(n_global_update);

		row_groups->Update(transaction, *this, FlatVector::GetData<row_t>(row_ids_slice), column_ids, updates_slice);
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

	if (!IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: attempting to update table \"%s\" but it has been %s by a different transaction",
		    GetTableName(), TableModification());
	}

	// now perform the actual update
	auto &transaction = DuckTransaction::Get(context, db);

	updates.Flatten();
	row_ids.Flatten(updates.size());
	row_groups->UpdateColumn(transaction, *this, row_ids, column_path, updates);
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

unique_ptr<BlockingSample> DataTable::GetSample() {
	return row_groups->GetSample();
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
	if (!HasIndexes()) {
		row_groups->SetAppendRequiresNewRowGroup();
	}
	// The row group payload data has been written. Now write:
	//   sample
	//   column stats
	//   row-group pointers
	//   table pointer
	//   index data
	writer.FinalizeTable(global_stats, *info, *row_groups, serializer);
}

void DataTable::CommitDropColumn(const idx_t column_index) {
	row_groups->CommitDropColumn(column_index);
}

void DataTable::Destroy() {
	row_groups->Destroy();
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
vector<ColumnSegmentInfo> DataTable::GetColumnSegmentInfo(const QueryContext &context) {
	auto lock = GetSharedCheckpointLock();
	return row_groups->GetColumnSegmentInfo(context);
}

//===--------------------------------------------------------------------===//
// Index Constraint Creation
//===--------------------------------------------------------------------===//
void DataTable::AddIndex(const ColumnList &columns, const vector<LogicalIndex> &column_indexes,
                         const IndexConstraintType type, const IndexStorageInfo &index_info) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to add an index to table \"%s\" but it has been "
		                           "%s by a different transaction",
		                           GetTableName(), TableModification());
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
