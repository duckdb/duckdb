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
#include "duckdb/storage/table/morsel.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"

#include "duckdb/common/chrono.hpp"

namespace duckdb {

DataTable::DataTable(DatabaseInstance &db, const string &schema, const string &table, vector<LogicalType> types_p,
                     unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(schema, table)), types(move(types_p)), db(db), total_rows(0), is_root(true) {
	// initialize the table with the existing data from disk, if any
	if (data && !data->column_data.empty()) {
		throw NotImplementedException("FIXME: persistent segment");
		// D_ASSERT(data->column_data.size() == types.size());
		// for (idx_t i = 0; i < types.size(); i++) {
		// 	if (i == 0) {
		// 		total_rows = data->column_data[i]->total_rows;
		// 	} else {
		// 		D_ASSERT(total_rows == data->column_data[i]->total_rows);
		// 	}
		// 	columns[i]->Initialize(*data->column_data[i]);
		// }
		// versions = move(data->versions);
	}
	if (total_rows == 0) {
		D_ASSERT(total_rows == 0);
		D_ASSERT(column_stats.empty());
		this->morsels = make_shared<SegmentTree>();
		AppendMorsel(0);

		for(auto &type : types) {
			column_stats.push_back(BaseStatistics::CreateEmpty(type));
		}
	} else {
		D_ASSERT(column_stats.size() == types.size());
	}
}

void DataTable::AppendMorsel(idx_t start_row) {
	auto new_morsel = make_unique<Morsel>(db, *info, start_row, 0);
	new_morsel->InitializeEmpty(types);
	morsels->AppendSegment(move(new_morsel));
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value)
    : info(parent.info), types(parent.types), db(parent.db), total_rows(parent.total_rows), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);
	// add the new column to this DataTable
	auto new_column_type = new_column.type;
	auto new_column_idx = parent.types.size();

	types.push_back(new_column_type);

	// set up the statistics
	for(idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]->Copy());
	}
	column_stats.push_back(BaseStatistics::CreateEmpty(new_column_type));

	auto &transaction = Transaction::GetTransaction(context);

	ExpressionExecutor executor;
	DataChunk dummy_chunk;
	Vector result(new_column_type);
	if (!default_value) {
		FlatVector::Validity(result).SetAllInvalid(STANDARD_VECTOR_SIZE);
	} else {
		executor.AddExpression(*default_value);
	}

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_unique<SegmentStatistics>(new_column.type);
	this->morsels = make_shared<SegmentTree>();
	auto current_morsel = (Morsel *) parent.morsels->GetRootSegment();
	while(current_morsel) {
		auto new_morsel = current_morsel->AddColumn(context, new_column, executor, default_value, result);
		// merge in the statistics
		column_stats[new_column_idx]->Merge(*new_morsel->GetStatistics(new_column_idx));

		morsels->AppendSegment(move(new_morsel));
		current_morsel = (Morsel *) current_morsel->next.get();
	}

	// also add this column to client local storage
	transaction.storage.AddColumn(&parent, this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), types(parent.types), db(parent.db), total_rows(parent.total_rows), is_root(true) {
	throw NotImplementedException("FIXME: remove column");
	// // prevent any new tuples from being added to the parent
	// lock_guard<mutex> parent_lock(parent.append_lock);
	// // first check if there are any indexes that exist that point to the removed column
	// for (auto &index : info->indexes) {
	// 	for (auto &column_id : index->column_ids) {
	// 		if (column_id == removed_column) {
	// 			throw CatalogException("Cannot drop this column: an index depends on it!");
	// 		} else if (column_id > removed_column) {
	// 			throw CatalogException("Cannot drop this column: an index depends on a column after it!");
	// 		}
	// 	}
	// }
	// // erase the column from this DataTable
	// D_ASSERT(removed_column < types.size());
	// types.erase(types.begin() + removed_column);
	// columns.erase(columns.begin() + removed_column);

	// // this table replaces the previous table, hence the parent is no longer the root DataTable
	// parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     vector<column_t> bound_columns, Expression &cast_expr)
    : info(parent.info), types(parent.types), db(parent.db), total_rows(parent.total_rows), is_root(true) {
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);

	// first check if there are any indexes that exist that point to the changed column
	for (auto &index : info->indexes) {
		for (auto &column_id : index->column_ids) {
			if (column_id == changed_idx) {
				throw CatalogException("Cannot change the type of this column: an index depends on it!");
			}
		}
	}
	// change the type in this DataTable
	types[changed_idx] = target_type;

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	for(idx_t i = 0; i < types.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(BaseStatistics::CreateEmpty(types[i]));
		} else {
			column_stats.push_back(parent.column_stats[i]->Copy());
		}
	}

	// scan the original table, and fill the new column with the transformed value
	auto &transaction = Transaction::GetTransaction(context);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.push_back(LOGICAL_ROW_TYPE);
		} else {
			scan_types.push_back(parent.types[bound_columns[i]]);
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(scan_types);

	ExpressionExecutor executor;
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.column_ids = move(bound_columns);

	// now alter the type of the column within all of the morsels individually
	this->morsels = make_shared<SegmentTree>();
	auto current_morsel = (Morsel *) parent.morsels->GetRootSegment();
	while(current_morsel) {
		auto new_morsel = current_morsel->AlterType(context, target_type, changed_idx, executor, scan_state, scan_chunk);
		column_stats[changed_idx]->Merge(*new_morsel->GetStatistics(changed_idx));
		morsels->AppendSegment(move(new_morsel));
		current_morsel = (Morsel *) current_morsel->next.get();
	}

	transaction.storage.ChangeType(&parent, this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	// initialize a column scan state for each column
	// initialize the chunk scan state
	auto morsel = (Morsel *) morsels->GetRootSegment();
	state.column_ids = column_ids;
	state.max_row = total_rows;
	state.table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(table_filters->filters.size() > 0);
		state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
	morsel->InitializeScan(state.morsel_scan_state);
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	transaction.storage.InitializeScan(this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids,
                                         TableFilterSet *table_filters, idx_t start_row, idx_t end_row) {
	throw NotImplementedException("FIXME: initialize scan");
	// D_ASSERT(start_row % STANDARD_VECTOR_SIZE == 0);
	// D_ASSERT(end_row > start_row);
	// // initialize a column scan state for each column
	// state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	// for (idx_t i = 0; i < column_ids.size(); i++) {
	// 	auto column = column_ids[i];
	// 	if (column != COLUMN_IDENTIFIER_ROW_ID) {
	// 		columns[column]->InitializeScanWithOffset(state.column_scans[i], start_row);
	// 	} else {
	// 		state.column_scans[i].current = nullptr;
	// 	}
	// }

	// // initialize the chunk scan state
	// state.column_count = column_ids.size();
	// state.current_row = start_row;
	// state.base_row = start_row;
	// state.max_row = end_row;
	// state.version_info = (MorselInfo *)versions->GetSegment(state.current_row);
	// state.table_filters = table_filters;
	// if (table_filters && !table_filters->filters.empty()) {
	// 	state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	// }
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = 100;
	if (context.force_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;

	return total_rows / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ParallelTableScanState &state) {
	state.current_row = 0;
	state.transaction_local_data = false;
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
                                 const vector<column_t> &column_ids) {
	throw NotImplementedException("FIXME: parallel scan");
	// idx_t parallel_scan_vector_count = 100;
	// if (context.force_parallelism) {
	// 	parallel_scan_vector_count = 1;
	// }
	// idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;

	// if (state.current_row < total_rows) {
	// 	idx_t next = MinValue(state.current_row + parallel_scan_tuple_count, total_rows);

	// 	// scan a morsel from the persistent rows
	// 	InitializeScanWithOffset(scan_state, column_ids, scan_state.table_filters, state.current_row, next);

	// 	state.current_row = next;
	// 	return true;
	// } else if (!state.transaction_local_data) {
	// 	auto &transaction = Transaction::GetTransaction(context);
	// 	// create a task for scanning the local data
	// 	scan_state.current_row = 0;
	// 	scan_state.base_row = 0;
	// 	scan_state.max_row = 0;
	// 	transaction.storage.InitializeScan(this, scan_state.local_state, scan_state.table_filters);
	// 	state.transaction_local_data = true;
	// 	return true;
	// } else {
	// 	// finished all scans: no more scans remaining
	// 	return false;
	// }
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, TableScanState &state, vector<column_t> &column_ids) {
	// scan the persistent segments
	if (ScanBaseTable(transaction, result, state, column_ids)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	transaction.storage.Scan(state.local_state, column_ids, result);
}

bool DataTable::CheckZonemap(TableScanState &state, const vector<column_t> &column_ids, TableFilterSet *table_filters,
                             idx_t &current_row) {
	return true;
	// if (!table_filters) {
	// 	return true;
	// }
	// for (auto &table_filter : table_filters->filters) {
	// 	for (auto &predicate_constant : table_filter.second) {
	// 		D_ASSERT(predicate_constant.column_index < column_ids.size());
	// 		auto base_column_idx = column_ids[predicate_constant.column_index];
	// 		bool read_segment = columns[base_column_idx]->CheckZonemap(
	// 		    state.column_scans[predicate_constant.column_index], predicate_constant);
	// 		if (!read_segment) {
	// 			//! We can skip this partition
	// 			idx_t vectors_to_skip =
	// 			    ceil((double)(state.column_scans[predicate_constant.column_index].current->count +
	// 			                  state.column_scans[predicate_constant.column_index].current->start - current_row) /
	// 			         STANDARD_VECTOR_SIZE);
	// 			for (idx_t i = 0; i < vectors_to_skip; ++i) {
	// 				state.NextVector();
	// 				current_row += STANDARD_VECTOR_SIZE;
	// 			}
	// 			return false;
	// 		}
	// 	}
	// }

	// return true;
}

bool DataTable::ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state,
                              const vector<column_t> &column_ids) {
	auto current_morsel = state.morsel_scan_state.morsel;
	while(current_morsel) {
		current_morsel->Scan(transaction, state.morsel_scan_state, result);
		if (result.size() > 0) {
			return true;
		} else {
			current_morsel = state.morsel_scan_state.morsel = (Morsel *) current_morsel->next.get();
			if (current_morsel) {
				current_morsel->InitializeScan(state.morsel_scan_state);
			}
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                      Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	throw NotImplementedException("FIXME: fetch");
	// // first figure out which row identifiers we should use for this transaction by looking at the VersionManagers
	// row_t rows[STANDARD_VECTOR_SIZE];
	// idx_t count = FetchRows(transaction, row_identifiers, fetch_count, rows);
	// if (count == 0) {
	// 	// no rows to use
	// 	return;
	// }
	// // for each of the remaining rows, now fetch the data
	// result.SetCardinality(count);
	// for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
	// 	auto column = column_ids[col_idx];
	// 	if (column == COLUMN_IDENTIFIER_ROW_ID) {
	// 		// row id column: fill in the row ids
	// 		D_ASSERT(result.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
	// 		result.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
	// 		auto data = FlatVector::GetData<row_t>(result.data[col_idx]);
	// 		for (idx_t i = 0; i < count; i++) {
	// 			data[i] = rows[i];
	// 		}
	// 	} else {
	// 		// regular column: fetch data from the base column
	// 		for (idx_t i = 0; i < count; i++) {
	// 			auto row_id = rows[i];
	// 			columns[column]->FetchRow(state, transaction, row_id, result.data[col_idx], i);
	// 		}
	// 	}
	// }
}

idx_t DataTable::FetchRows(Transaction &transaction, Vector &row_identifiers, idx_t fetch_count, row_t result_rows[]) {
	throw NotImplementedException("FIXME: fetch rows");
	// D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);

	// // now iterate over the row ids and figure out which rows to use
	// idx_t count = 0;

	// auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	// for (idx_t i = 0; i < fetch_count; i++) {
	// 	auto row_id = row_ids[i];
	// 	auto segment = (MorselInfo *)versions->GetSegment(row_id);
	// 	bool use_row = segment->Fetch(transaction, row_id - segment->start);
	// 	if (use_row) {
	// 		// row is not deleted; use the row
	// 		result_rows[count++] = row_id;
	// 	}
	// }
	// return count;
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
	} catch (Exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, ex.what());
	} catch (...) {
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	}
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
			for (auto &index : info->indexes) {
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
	if (chunk.ColumnCount() != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
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
	state.append_lock = std::unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	// obtain locks on all indexes for the table
	state.index_locks = unique_ptr<IndexLock[]>(new IndexLock[info->indexes.size()]);
	for (idx_t i = 0; i < info->indexes.size(); i++) {
		info->indexes[i]->InitializeLock(state.index_locks[i]);
	}
	state.row_start = total_rows;
	state.current_row = state.row_start;
	state.remaining_append_count = append_count;

	// start writing to the morsels
	lock_guard<mutex> morsel_lock(morsels->node_lock);
	auto last_morsel = (Morsel *) morsels->GetLastSegment();
	D_ASSERT(total_rows == last_morsel->start + last_morsel->count);
	last_morsel->InitializeAppend(transaction, state.morsel_append_state, state.remaining_append_count);
	total_rows += append_count;
}

void DataTable::Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	D_ASSERT(chunk.ColumnCount() == types.size());
	chunk.Verify();

	idx_t remaining = chunk.size();
	while(true) {
		auto current_morsel = state.morsel_append_state.morsel;
		// check how much we can fit into the current morsel
		idx_t append_count = MinValue<idx_t>(remaining, Morsel::MORSEL_SIZE - state.morsel_append_state.offset_in_morsel);
		if (append_count > 0) {
			current_morsel->Append(state.morsel_append_state, chunk, append_count);
			// merge the stats
			for (idx_t i = 0; i < types.size(); i++) {
				column_stats[i]->Merge(*current_morsel->GetStatistics(i));
			}
		}
		state.remaining_append_count -= append_count;
		remaining -= append_count;
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one morsel)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for(idx_t i = 0; i < remaining; i++) {
					sel.set_index(i, append_count + i);
				}
				chunk.Slice(sel, remaining);
			}
			// append a new morsel
			AppendMorsel(current_morsel->start + current_morsel->count);
			// set up the append state for this morsel
			lock_guard<mutex> morsel_lock(morsels->node_lock);
			auto last_morsel = (Morsel *) morsels->GetLastSegment();
			last_morsel->InitializeAppend(transaction, state.morsel_append_state, state.remaining_append_count);
			continue;
		} else {
			break;
		}
	}
	state.current_row += chunk.size();
}

void DataTable::ScanTableSegment(idx_t row_start, idx_t count, const std::function<void(DataChunk &chunk)> &function) {
	throw NotImplementedException("FIXME: scan table segment");
	// idx_t end = row_start + count;

	// vector<column_t> column_ids;
	// vector<LogicalType> types;
	// for (idx_t i = 0; i < columns.size(); i++) {
	// 	column_ids.push_back(i);
	// 	types.push_back(columns[i]->type);
	// }
	// DataChunk chunk;
	// chunk.Initialize(types);

	// CreateIndexScanState state;

	// idx_t row_start_aligned = row_start / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
	// InitializeScanWithOffset(state, column_ids, nullptr, row_start_aligned, row_start + count);

	// while (true) {
	// 	idx_t current_row = state.current_row;
	// 	CreateIndexScan(state, column_ids, chunk, true);
	// 	if (chunk.size() == 0) {
	// 		break;
	// 	}
	// 	idx_t end_row = state.current_row;
	// 	// figure out if we need to write the entire chunk or just part of it
	// 	idx_t chunk_start = current_row < row_start ? row_start : current_row;
	// 	idx_t chunk_end = end_row > end ? end : end_row;
	// 	idx_t chunk_count = chunk_end - chunk_start;
	// 	if (chunk_count != chunk.size()) {
	// 		// need to slice the chunk before insert
	// 		SelectionVector sel(chunk_start % STANDARD_VECTOR_SIZE, chunk_count);
	// 		chunk.Slice(sel, chunk_count);
	// 	}
	// 	function(chunk);
	// 	chunk.Reset();
	// }
}

void DataTable::WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count) {
	log.WriteSetTable(info->schema, info->table);
	ScanTableSegment(row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	auto morsel = (Morsel *) morsels->GetSegment(row_start);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_morsel = current_row - morsel->start;
		idx_t append_count = MinValue<idx_t>(morsel->count - start_in_morsel, remaining);

		morsel->CommitAppend(commit_id, start_in_morsel, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		morsel = (Morsel *) morsel->next.get();
	}
	info->cardinality += count;
}

void DataTable::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (count == 0) {
		// nothing to revert!
		return;
	}
	throw NotImplementedException("FIXME: revert append internal");
	// if (total_rows != start_row + count) {
	// 	// interleaved append: don't do anything
	// 	// in this case the rows will stay as "inserted by transaction X", but will never be committed
	// 	// they will never be used by any other transaction and will essentially leave a gap
	// 	// this situation is rare, and as such we don't care about optimizing it (yet?)
	// 	// it only happens if C1 appends a lot of data -> C2 appends a lot of data -> C1 rolls back
	// 	return;
	// }
	// // adjust the cardinality
	// info->cardinality = start_row;
	// total_rows = start_row;
	// D_ASSERT(is_root);
	// // revert changes in the base columns
	// for (idx_t i = 0; i < types.size(); i++) {
	// 	columns[i]->RevertAppend(start_row);
	// }
	// // revert appends made to morsels
	// lock_guard<mutex> tree_lock(versions->node_lock);
	// // find the segment index that the current row belongs to
	// idx_t segment_index = versions->GetSegmentIndex(start_row);
	// auto segment = versions->nodes[segment_index].node;
	// auto &info = (MorselInfo &)*segment;

	// // remove any segments AFTER this segment: they should be deleted entirely
	// if (segment_index < versions->nodes.size() - 1) {
	// 	versions->nodes.erase(versions->nodes.begin() + segment_index + 1, versions->nodes.end());
	// }
	// info.next = nullptr;
	// info.RevertAppend(start_row);
}

void DataTable::RevertAppend(idx_t start_row, idx_t count) {
	throw NotImplementedException("FIXME: revert append");
	// lock_guard<mutex> lock(append_lock);
	// if (!info->indexes.empty()) {
	// 	auto index_locks = unique_ptr<IndexLock[]>(new IndexLock[info->indexes.size()]);
	// 	for (idx_t i = 0; i < info->indexes.size(); i++) {
	// 		info->indexes[i]->InitializeLock(index_locks[i]);
	// 	}
	// 	idx_t current_row_base = start_row;
	// 	row_t row_data[STANDARD_VECTOR_SIZE];
	// 	Vector row_identifiers(LOGICAL_ROW_TYPE, (data_ptr_t)row_data);
	// 	ScanTableSegment(start_row, count, [&](DataChunk &chunk) {
	// 		for (idx_t i = 0; i < chunk.size(); i++) {
	// 			row_data[i] = current_row_base + i;
	// 		}
	// 		for (idx_t i = 0; i < info->indexes.size(); i++) {
	// 			info->indexes[i]->Delete(index_locks[i], chunk, row_identifiers);
	// 		}
	// 		current_row_base += chunk.size();
	// 	});
	// }
	// RevertAppendInternal(start_row, count);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
bool DataTable::AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.empty()) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LOGICAL_ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	idx_t failed_index = INVALID_INDEX;
	// now append the entries to the indices
	for (idx_t i = 0; i < info->indexes.size(); i++) {
		if (!info->indexes[i]->Append(state.index_locks[i], chunk, row_identifiers)) {
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (idx_t i = 0; i < failed_index; i++) {
			info->indexes[i]->Delete(state.index_locks[i], chunk, row_identifiers);
		}
		return false;
	}
	return true;
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.empty()) {
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
	for (idx_t i = 0; i < info->indexes.size(); i++) {
		info->indexes[i]->Delete(state.index_locks[i], chunk, row_identifiers);
	}
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	throw NotImplementedException("FIXME: remove from indexes");
	// D_ASSERT(is_root);
	// auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	// // create a selection vector from the row_ids
	// SelectionVector sel(STANDARD_VECTOR_SIZE);
	// for (idx_t i = 0; i < count; i++) {
	// 	sel.set_index(i, row_ids[i] % STANDARD_VECTOR_SIZE);
	// }

	// // fetch the data for these row identifiers
	// DataChunk result;
	// result.Initialize(types);
	// // FIXME: we do not need to fetch all columns, only the columns required by the indices!
	// auto states = unique_ptr<ColumnScanState[]>(new ColumnScanState[types.size()]);
	// for (idx_t i = 0; i < types.size(); i++) {
	// 	columns[i]->Fetch(states[i], row_ids[0], result.data[i]);
	// }
	// result.Slice(sel, count);
	// for (auto &index : info->indexes) {
	// 	index->Delete(result, row_identifiers);
	// }
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return;
	}

	auto &transaction = Transaction::GetTransaction(context);

	row_identifiers.Normalify(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);
	auto first_id = ids[0];

	if (first_id >= MAX_ROW_ID) {
		// deletion is in transaction-local storage: push delete into local chunk collection
		transaction.storage.Delete(this, row_identifiers, count);
	} else {
		auto morsel = (Morsel *) morsels->GetSegment(first_id);
		morsel->Delete(transaction, this, row_identifiers, count);
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, vector<column_t> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i]].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, vector<column_t> &column_ids,
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

void DataTable::VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids) {
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
	for (auto &index : info->indexes) {
		D_ASSERT(!index->IndexIsUpdated(column_ids));
	}
#endif
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, vector<column_t> &column_ids,
                       DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);

	updates.Verify();
	if (updates.size() == 0) {
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
	row_ids.Normalify(updates.size());
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		// update is in transaction-local storage: push update into local storage
		transaction.storage.Update(this, row_ids, column_ids, updates);
		return;
	}
	// find the morsel this id belongs to
	auto morsel = (Morsel *) morsels->GetSegment(first_id);
	morsel->Update(transaction, updates, row_ids, column_ids);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_id = column_ids[i];
		column_stats[column_id]->Merge(*morsel->GetStatistics(column_id));
	}
}

//===--------------------------------------------------------------------===//
// Create Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids) {
	throw NotImplementedException("FIXME: create index scan");
	// // we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	// state.append_lock = std::unique_lock<mutex>(append_lock);
	// state.delete_lock = std::unique_lock<mutex>(versions->node_lock);

	// InitializeScan(state, column_ids);
}

void DataTable::CreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids, DataChunk &result,
                                bool allow_pending_updates) {
	throw NotImplementedException("FIXME: create index scan");
	// // scan the persistent segments
	// if (ScanCreateIndex(state, column_ids, result, state.current_row, state.max_row, allow_pending_updates)) {
	// 	return;
	// }
}

bool DataTable::ScanCreateIndex(CreateIndexScanState &state, const vector<column_t> &column_ids, DataChunk &result,
                                idx_t &current_row, idx_t max_row, bool allow_pending_updates) {
	throw NotImplementedException("FIXME: scan create index scan");
	// if (current_row >= max_row) {
	// 	return false;
	// }
	// idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, max_row - current_row);

	// // scan the base columns to fetch the actual data
	// // note that we insert all data into the index, even if it is marked as deleted
	// // FIXME: tuples that are already "cleaned up" do not need to be inserted into the index!
	// for (idx_t i = 0; i < column_ids.size(); i++) {
	// 	auto column = column_ids[i];
	// 	if (column == COLUMN_IDENTIFIER_ROW_ID) {
	// 		// scan row id
	// 		D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
	// 		result.data[i].Sequence(current_row, 1);
	// 	} else {
	// 		// scan actual base column
	// 		columns[column]->IndexScan(state.column_scans[i], result.data[i], allow_pending_updates);
	// 	}
	// }
	// result.SetCardinality(count);

	// current_row += STANDARD_VECTOR_SIZE;
	// return count > 0;
}

void DataTable::AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions) {
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
	IndexLock lock;
	index->InitializeLock(lock);
	ExpressionExecutor executor(expressions);
	while (true) {
		intermediate.Reset();
		// scan a new chunk from the table to index
		CreateIndexScan(state, column_ids, intermediate);
		if (intermediate.size() == 0) {
			// finished scanning for index creation
			// release all locks
			break;
		}
		// resolve the expressions for this chunk
		executor.Execute(intermediate, result);

		// insert into the index
		if (!index->Insert(lock, result, intermediate.data[intermediate.ColumnCount() - 1])) {
			throw ConstraintException("Cant create unique index, table contains duplicate data on indexed column(s)");
		}
	}
	info->indexes.push_back(move(index));
}

unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return column_stats[column_id]->Copy();
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void DataTable::Checkpoint(TableDataWriter &writer) {
	throw NotImplementedException("FIXME: checkpoint");
	// // checkpoint each individual column
	// for (size_t i = 0; i < columns.size(); i++) {
	// 	columns[i]->Checkpoint(writer);
	// }
}

void DataTable::CheckpointDeletes(TableDataWriter &writer) {
	throw NotImplementedException("FIXME: checkpoint deletes");
	// // then we checkpoint the deleted tuples
	// D_ASSERT(versions);
	// writer.CheckpointDeletes(((MorselInfo *)versions->GetRootSegment()));
}

void DataTable::CommitDropColumn(idx_t index) {
	throw NotImplementedException("FIXME: commit drop column");
	// columns[index]->CommitDropColumn();
}

idx_t DataTable::GetTotalRows() {
	return total_rows;
}

void DataTable::CommitDropTable() {
	throw NotImplementedException("FIXME: commit drop table");
	// // commit a drop of this table: mark all blocks as modified so they can be reclaimed later on
	// for (size_t i = 0; i < columns.size(); i++) {
	// 	CommitDropColumn(i);
	// }
}

} // namespace duckdb
