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
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

DataTable::DataTable(DatabaseInstance &db, const string &schema, const string &table,
                     vector<ColumnDefinition> column_definitions_p, unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(db, schema, table)), column_definitions(move(column_definitions_p)), db(db),
      total_rows(0), is_root(true) {
	// initialize the table with the existing data from disk, if any
	this->row_groups = make_shared<SegmentTree>();
	auto types = GetTypes();
	if (data && !data->row_groups.empty()) {
		for (auto &row_group_pointer : data->row_groups) {
			auto new_row_group = make_unique<RowGroup>(db, *info, types, row_group_pointer);
			auto row_group_count = new_row_group->start + new_row_group->count;
			if (row_group_count > total_rows) {
				total_rows = row_group_count;
			}
			row_groups->AppendSegment(move(new_row_group));
		}
		column_stats.reserve(data->column_stats.size());
		for (auto &stats : data->column_stats) {
			column_stats.push_back(make_shared<ColumnStatistics>(move(stats)));
		}
		if (column_stats.size() != types.size()) { // LCOV_EXCL_START
			throw IOException("Table statistics column count is not aligned with table column count. Corrupt file?");
		} // LCOV_EXCL_STOP
	}
	if (column_stats.empty()) {
		D_ASSERT(total_rows == 0);

		AppendRowGroup(0);
		for (auto &type : types) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(type));
		}
	} else {
		D_ASSERT(column_stats.size() == types.size());
		D_ASSERT(row_groups->GetRootSegment() != nullptr);
	}
}

void DataTable::AppendRowGroup(idx_t start_row) {
	auto types = GetTypes();
	auto new_row_group = make_unique<RowGroup>(db, *info, start_row, 0);
	new_row_group->InitializeEmpty(types);
	row_groups->AppendSegment(move(new_row_group));
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);
	// add the new column to this DataTable
	auto new_column_type = new_column.Type();
	auto new_column_idx = parent.column_definitions.size();

	// set up the statistics
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
	column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_column_type));

	// add the column definitions from this DataTable
	column_definitions.emplace_back(new_column.Copy());

	auto &transaction = Transaction::GetTransaction(context);

	ExpressionExecutor executor(Allocator::Get(context));
	DataChunk dummy_chunk;
	Vector result(new_column_type);
	if (!default_value) {
		FlatVector::Validity(result).SetAllInvalid(STANDARD_VECTOR_SIZE);
	} else {
		executor.AddExpression(*default_value);
	}

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_unique<SegmentStatistics>(new_column.Type());
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->AddColumn(context, new_column, executor, default_value, result);
		// merge in the statistics
		column_stats[new_column_idx]->stats->Merge(*new_row_group->GetStatistics(new_column_idx));

		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	// also add this column to client local storage
	transaction.storage.AddColumn(&parent, this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
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

	// erase the stats from this DataTable
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i != removed_column) {
			column_stats.push_back(parent.column_stats[i]);
		}
	}

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
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->RemoveColumn(removed_column);
		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

// Alter column to add new constraint
DataTable::DataTable(ClientContext &context, DataTable &parent, unique_ptr<Constraint> constraint)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), row_groups(parent.row_groups),
      is_root(true) {

	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}

	// Verify the new constraint against current persistent/local data
	VerifyNewConstraint(context, parent, constraint.get());

	// Get the local data ownership from old dt
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.MoveStorage(&parent, this);
	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     vector<column_t> bound_columns, Expression &cast_expr)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
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
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(column_definitions[i].Type()));
		} else {
			column_stats.push_back(parent.column_stats[i]);
		}
	}

	// scan the original table, and fill the new column with the transformed value
	auto &transaction = Transaction::GetTransaction(context);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(parent.column_definitions[bound_columns[i]].Type());
		}
	}
	auto &allocator = Allocator::Get(context);
	DataChunk scan_chunk;
	scan_chunk.Initialize(allocator, scan_types);

	ExpressionExecutor executor(allocator);
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.column_ids = bound_columns;
	scan_state.max_row = total_rows;

	// now alter the type of the column within all of the row_groups individually
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group =
		    current_row_group->AlterType(context, target_type, changed_idx, executor, scan_state, scan_chunk);
		column_stats[changed_idx]->stats->Merge(*new_row_group->GetStatistics(changed_idx));
		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	transaction.storage.ChangeType(&parent, this, changed_idx, target_type, bound_columns, cast_expr);

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

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	// initialize a column scan state for each column
	// initialize the chunk scan state
	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	state.column_ids = column_ids;
	state.max_row = total_rows;
	state.table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(!table_filters->filters.empty());
		state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
	while (row_group && !row_group->InitializeScan(state.row_group_scan_state)) {
		row_group = (RowGroup *)row_group->next.get();
	}
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	transaction.storage.InitializeScan(this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
                                         idx_t end_row) {

	auto row_group = (RowGroup *)row_groups->GetSegment(start_row);
	state.column_ids = column_ids;
	state.max_row = end_row;
	state.table_filters = nullptr;
	idx_t start_vector = (start_row - row_group->start) / STANDARD_VECTOR_SIZE;
	if (!row_group->InitializeScanWithOffset(state.row_group_scan_state, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool DataTable::InitializeScanInRowGroup(TableScanState &state, const vector<column_t> &column_ids,
                                         TableFilterSet *table_filters, RowGroup *row_group, idx_t vector_index,
                                         idx_t max_row) {
	state.column_ids = column_ids;
	state.max_row = max_row;
	state.table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(!table_filters->filters.empty());
		state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
	return row_group->InitializeScanWithOffset(state.row_group_scan_state, vector_index);
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = RowGroup::ROW_GROUP_VECTOR_COUNT;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;

	return total_rows / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	state.current_row_group = (RowGroup *)row_groups->GetRootSegment();
	state.transaction_local_data = false;
	// figure out the max row we can scan for both the regular and the transaction-local storage
	state.max_row = total_rows;
	state.vector_index = 0;
	state.local_state.max_index = 0;
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.InitializeScan(this, state.local_state, nullptr);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
                                 const vector<column_t> &column_ids) {
	while (state.current_row_group) {
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
		bool need_to_scan;
		if (state.current_row_group->count == 0) {
			need_to_scan = false;
		} else {
			need_to_scan = InitializeScanInRowGroup(scan_state, column_ids, scan_state.table_filters,
			                                        state.current_row_group, vector_index, max_row);
		}
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
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (VectorOperations::HasNull(vector, count)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
	}
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(TableCatalogEntry &table, DataChunk &chunk, Expression &expr,
                                             column_t index) {
	auto &col = table.columns[index];
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(Allocator::DefaultAllocator(), expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("Incorrect %s value for generated column \"%s\"", col.Type().ToString(), col.Name());
	}
}

static void VerifyCheckConstraint(TableCatalogEntry &table, Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(Allocator::DefaultAllocator(), expr);
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

bool DataTable::IsForeignKeyIndex(const vector<idx_t> &fk_keys, Index &index, ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.column_ids.size()) {
		return false;
	}
	for (auto &fk_key : fk_keys) {
		bool is_found = false;
		for (auto &index_key : index.column_ids) {
			if (fk_key == index_key) {
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

static void VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context, DataChunk &chunk,
                                       bool is_append) {
	const vector<idx_t> *src_keys_ptr = &bfk.info.fk_keys;
	const vector<idx_t> *dst_keys_ptr = &bfk.info.pk_keys;
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
	for (idx_t i = 0; i < table_entry_ptr->columns.size(); i++) {
		types.emplace_back(table_entry_ptr->columns[i].Type());
	}
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr->size(); i++) {
		dst_chunk.data[(*dst_keys_ptr)[i]].Reference(chunk.data[(*src_keys_ptr)[i]]);
	}
	dst_chunk.SetCardinality(chunk.size());
	auto data_table = table_entry_ptr->storage.get();

	idx_t count = dst_chunk.size();
	if (count <= 0) {
		return;
	}

	// we need to look at the error messages concurrently in data table's index and transaction local storage's index
	vector<string> err_msgs, tran_err_msgs;
	err_msgs.resize(count);
	tran_err_msgs.resize(count);

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	auto index = data_table->info->indexes.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	if (!index) {
		throw InternalException("Internal Foreign Key error: could not find index to verify...");
	}
	if (is_append) {
		index->VerifyAppendForeignKey(dst_chunk, err_msgs.data());
	} else {
		index->VerifyDeleteForeignKey(dst_chunk, err_msgs.data());
	}
	// check whether or not the chunk can be inserted or deleted into the referenced table' transaction local storage
	auto &transaction = Transaction::GetTransaction(context);
	bool transaction_check = transaction.storage.Find(data_table);
	if (transaction_check) {
		vector<unique_ptr<Index>> &transact_index_vec = transaction.storage.GetIndexes(data_table);
		for (idx_t i = 0; i < transact_index_vec.size(); i++) {
			if (DataTable::IsForeignKeyIndex(*dst_keys_ptr, *transact_index_vec[i], fk_type)) {
				if (is_append) {
					transact_index_vec[i]->VerifyAppendForeignKey(dst_chunk, tran_err_msgs.data());
				} else {
					transact_index_vec[i]->VerifyDeleteForeignKey(dst_chunk, tran_err_msgs.data());
				}
			}
		}
	}

	// we need to look at the error messages concurrently in data table's index and transaction local storage's index
	for (idx_t i = 0; i < count; i++) {
		if (!transaction_check) {
			// if there is no transaction-local data we only need to check if there is an error message in the main
			// index
			if (!err_msgs[i].empty()) {
				throw ConstraintException(err_msgs[i]);
			} else {
				continue;
			}
		}
		if (is_append) {
			// if we are appending we need to check to ensure the foreign key exists in either the transaction-local
			// storage or the main table
			if (!err_msgs[i].empty() && !tran_err_msgs[i].empty()) {
				throw ConstraintException(err_msgs[i]);
			} else {
				continue;
			}
		}
		// if we are deleting we need to ensure the foreign key DOES NOT exist in EITHER the transaction-local storage
		// OR the main table
		if (!err_msgs[i].empty() || !tran_err_msgs[i].empty()) {
			string &err_msg = err_msgs[i];
			if (err_msg.empty()) {
				err_msg = tran_err_msgs[i];
			}
			throw ConstraintException(err_msg);
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

void DataTable::VerifyNewConstraint(ClientContext &context, DataTable &parent, const Constraint *constraint) {
	if (constraint->type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}
	// scan the original table, check if there's any null value
	auto &not_null_constraint = (NotNullConstraint &)*constraint;
	auto &transaction = Transaction::GetTransaction(context);
	vector<LogicalType> scan_types;
	scan_types.push_back(parent.column_definitions[not_null_constraint.index].Type());
	DataChunk scan_chunk;
	auto &allocator = Allocator::Get(context);
	scan_chunk.Initialize(allocator, scan_types);

	CreateIndexScanState state;
	vector<column_t> cids;
	cids.push_back(not_null_constraint.index);
	// Use ScanCreateIndex to scan the latest committed data
	InitializeCreateIndexScan(state, cids);
	while (true) {
		scan_chunk.Reset();
		ScanCreateIndex(state, scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (scan_chunk.size() == 0) {
			break;
		}
		// Check constraint
		if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
			throw ConstraintException("NOT NULL constraint failed: %s.%s", info->table,
			                          column_definitions[not_null_constraint.index].GetName());
		}
	}

	TableScanState scan_state;
	scan_state.column_ids.push_back(not_null_constraint.index);
	scan_state.max_row = total_rows;

	// For local storage
	transaction.storage.InitializeScan(&parent, scan_state.local_state, nullptr);
	if (scan_state.local_state.GetStorage()) {
		while (scan_state.local_state.chunk_index <= scan_state.local_state.max_index) {
			scan_chunk.Reset();
			transaction.storage.Scan(scan_state.local_state, scan_state.column_ids, scan_chunk);
			if (scan_chunk.size() == 0) {
				break;
			}
			// Check constraint
			if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", info->table,
				                          column_definitions[not_null_constraint.index].GetName());
			}
		}
	}
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	auto binder = Binder::CreateBinder(context);
	auto bound_columns = unordered_set<column_t>();
	CheckBinder generated_check_binder(*binder, context, table.name, table.columns, bound_columns);
	for (idx_t i = 0; i < table.columns.size(); i++) {
		auto &col = table.columns[i];
		if (!col.Generated()) {
			continue;
		}
		D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
		generated_check_binder.target_type = col.Type();
		auto to_be_bound_expression = col.GeneratedExpression().Copy();
		auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
		VerifyGeneratedExpressionSuccess(table, chunk, *bound_expression, i);
	}
	for (idx_t i = 0; i < table.bound_constraints.size(); i++) {
		auto &base_constraint = table.constraints[i];
		auto &constraint = table.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			VerifyNotNullConstraint(table, chunk.data[not_null.index], chunk.size(),
			                        table.columns[not_null.index].Name());
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

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	// FIXME: could be an assertion instead?
	if (chunk.ColumnCount() != table.StandardColumnCount()) {
		throw InternalException("Mismatch in column count for append");
	}
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, context, chunk);

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
	state.row_start = total_rows;
	state.current_row = state.row_start;
	state.remaining_append_count = append_count;

	// start writing to the row_groups
	lock_guard<mutex> row_group_lock(row_groups->node_lock);
	auto last_row_group = (RowGroup *)row_groups->GetLastSegment();
	D_ASSERT(total_rows == last_row_group->start + last_row_group->count);
	last_row_group->InitializeAppend(transaction, state.row_group_append_state, state.remaining_append_count);
	total_rows += append_count;
}

void DataTable::Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	D_ASSERT(chunk.ColumnCount() == column_definitions.size());
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
			lock_guard<mutex> stats_guard(stats_lock);
			for (idx_t i = 0; i < column_definitions.size(); i++) {
				current_row_group->MergeIntoStatistics(i, *column_stats[i]->stats);
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
				SelectionVector sel(STANDARD_VECTOR_SIZE);
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
	for (idx_t col_idx = 0; col_idx < column_stats.size(); col_idx++) {
		auto type = chunk.data[col_idx].GetType().InternalType();
		if (type == PhysicalType::LIST || type == PhysicalType::STRUCT) {
			continue;
		}
		column_stats[col_idx]->stats->UpdateDistinctStatistics(chunk.data[col_idx], chunk.size());
	}
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
	if (log.skip_writing) {
		return;
	}
	log.WriteSetTable(info->schema, info->table);
	ScanTableSegment(row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);

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
	info->cardinality += count;
}

void DataTable::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (count == 0) {
		// nothing to revert!
		return;
	}
	if (total_rows != start_row + count) {
		// interleaved append: don't do anything
		// in this case the rows will stay as "inserted by transaction X", but will never be committed
		// they will never be used by any other transaction and will essentially leave a gap
		// this situation is rare, and as such we don't care about optimizing it (yet?)
		// it only happens if C1 appends a lot of data -> C2 appends a lot of data -> C1 rolls back
		return;
	}
	// adjust the cardinality
	info->cardinality = start_row;
	total_rows = start_row;
	D_ASSERT(is_root);
	// revert appends made to row_groups
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
bool DataTable::AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
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
	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	TableScanState state;
	state.max_row = total_rows;
	auto types = GetTypes();
	for (idx_t i = 0; i < types.size(); i++) {
		state.column_ids.push_back(i);
	}
	DataChunk result;
	result.Initialize(Allocator::Get(db), types);

	row_group->InitializeScanWithOffset(state.row_group_scan_state, row_group_vector_idx);
	row_group->ScanCommitted(state.row_group_scan_state, result,
	                         TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES);
	result.Slice(sel, count);

	info->indexes.Scan([&](Index &index) {
		index.Delete(result, row_identifiers);
		return false;
	});
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

	row_identifiers.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);
	auto first_id = ids[0];

	// verify any constraints on the delete rows
	DataChunk verify_chunk;
	if (first_id >= MAX_ROW_ID) {
		transaction.storage.FetchChunk(this, row_identifiers, count, verify_chunk);
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
		return transaction.storage.Delete(this, row_identifiers, count);
	} else {
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
			delete_count += row_group->Delete(transaction, this, ids + start, pos - start);
		} while (pos < count);
		return delete_count;
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
		// FIXME: not all columns in UPDATE clause are present!
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
					VerifyNotNullConstraint(table, chunk.data[i], chunk.size(), table.columns[not_null.index].Name());
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

	updates.Flatten();
	row_ids.Flatten(count);
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
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = (RowGroup *)row_groups->GetSegment(ids[pos]);
		row_t base_id =
		    row_group->start + ((ids[pos] - row_group->start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
		for (pos++; pos < count; pos++) {
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

		lock_guard<mutex> stats_guard(stats_lock);
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			column_stats[column_id]->stats->Merge(*row_group->GetStatistics(column_id));
		}
	} while (pos < count);
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
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		throw NotImplementedException("Cannot update a column-path on transaction local data");
	}
	// find the row_group this id belongs to
	auto primary_column_idx = column_path[0];
	auto row_group = (RowGroup *)row_groups->GetSegment(first_id);
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	lock_guard<mutex> stats_guard(stats_lock);
	column_stats[primary_column_idx]->stats->Merge(*row_group->GetStatistics(primary_column_idx));
}

//===--------------------------------------------------------------------===//
// Create Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids) {
	// we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	state.append_lock = std::unique_lock<mutex>(append_lock);
	state.delete_lock = std::unique_lock<mutex>(row_groups->node_lock);

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
	auto &allocator = Allocator::Get(db);

	DataChunk result;
	result.Initialize(allocator, index->logical_types);

	DataChunk intermediate;
	vector<LogicalType> intermediate_types;
	auto column_ids = index->column_ids;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	for (auto &id : index->column_ids) {
		auto &col = column_definitions[id];
		intermediate_types.push_back(col.Type());
	}
	intermediate_types.emplace_back(LogicalType::ROW_TYPE);
	intermediate.Initialize(allocator, intermediate_types);

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
		ExpressionExecutor executor(allocator, expressions);
		while (true) {
			intermediate.Reset();
			result.Reset();
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
	lock_guard<mutex> stats_guard(stats_lock);
	return column_stats[column_id]->stats->Copy();
}

void DataTable::SetStatistics(column_t column_id, const std::function<void(BaseStatistics &)> &set_fun) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	lock_guard<mutex> stats_guard(stats_lock);
	set_fun(*column_stats[column_id]->stats);
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void DataTable::Checkpoint(TableDataWriter &writer) {
	// checkpoint each individual row group
	// FIXME: we might want to combine adjacent row groups in case they have had deletions...
	vector<unique_ptr<BaseStatistics>> global_stats;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		global_stats.push_back(column_stats[i]->stats->Copy());
	}

	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	vector<RowGroupPointer> row_group_pointers;
	while (row_group) {
		auto pointer = row_group->Checkpoint(writer, global_stats);
		row_group_pointers.push_back(move(pointer));
		row_group = (RowGroup *)row_group->next.get();
	}
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto &data_writer = writer.GetTableWriter();
	auto pointer = data_writer.GetBlockPointer();

	for (auto &stats : global_stats) {
		stats->Serialize(data_writer);
	}
	// now start writing the row group pointers to disk
	data_writer.Write<uint64_t>(row_group_pointers.size());
	for (auto &row_group_pointer : row_group_pointers) {
		RowGroup::Serialize(row_group_pointer, data_writer);
	}
	// Now we serialize indexes in the tabledata_writer
	auto blocks_info = info->indexes.SerializeIndexes(data_writer);

	// metadata writing time
	auto &metadata_writer = writer.GetMetaWriter();

	// write the block pointer for the table info
	metadata_writer.Write<block_id_t>(pointer.block_id);
	metadata_writer.Write<uint64_t>(pointer.offset);

	// Write-off block ids and offsets of indexes
	metadata_writer.Write<idx_t>(blocks_info.size());
	for (auto &block_info : blocks_info) {
		metadata_writer.Write<idx_t>(block_info.block_id);
		metadata_writer.Write<idx_t>(block_info.offset);
	}
}

void DataTable::CommitDropColumn(idx_t index) {
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDropColumn(index);
		segment = (RowGroup *)segment->next.get();
	}
}

idx_t DataTable::GetTotalRows() {
	return total_rows;
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified so they can be reclaimed later on
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDrop();
		segment = (RowGroup *)segment->next.get();
	}
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
vector<vector<Value>> DataTable::GetStorageInfo() {
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

} // namespace duckdb
