#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

PhysicalInsert::PhysicalInsert(vector<LogicalType> types_p, TableCatalogEntry *table,
                               physical_index_vector_t<idx_t> column_index_map,
                               vector<unique_ptr<Expression>> bound_defaults,
                               vector<unique_ptr<Expression>> set_expressions, vector<PhysicalIndex> set_columns,
                               vector<LogicalType> set_types, idx_t estimated_cardinality, bool return_chunk,
                               bool parallel, OnConflictAction action_type,
                               unique_ptr<Expression> on_conflict_condition_p,
                               unique_ptr<Expression> do_update_condition_p, unordered_set<column_t> conflict_target_p,
                               vector<column_t> columns_to_fetch_p)
    : PhysicalOperator(PhysicalOperatorType::INSERT, std::move(types_p), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(table), insert_types(table->GetTypes()),
      bound_defaults(std::move(bound_defaults)), return_chunk(return_chunk), parallel(parallel),
      action_type(action_type), set_expressions(std::move(set_expressions)), set_columns(std::move(set_columns)),
      set_types(std::move(set_types)), on_conflict_condition(std::move(on_conflict_condition_p)),
      do_update_condition(std::move(do_update_condition_p)), conflict_target(std::move(conflict_target_p)),
      columns_to_fetch(std::move(columns_to_fetch_p)) {

	if (action_type == OnConflictAction::THROW) {
		return;
	}

	D_ASSERT(set_expressions.size() == set_columns.size());

	// One or more columns are referenced from the existing table,
	// we use the 'insert_types' to figure out which types these columns have
	types_to_fetch = vector<LogicalType>(columns_to_fetch.size(), LogicalType::SQLNULL);
	for (idx_t i = 0; i < columns_to_fetch.size(); i++) {
		auto &id = columns_to_fetch[i];
		D_ASSERT(id < insert_types.size());
		types_to_fetch[i] = insert_types[id];
	}
}

PhysicalInsert::PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      return_chunk(false), schema(schema), info(std::move(info_p)), parallel(parallel),
      action_type(OnConflictAction::THROW) {
	GetInsertInfo(*info, insert_types, bound_defaults);
}

void PhysicalInsert::GetInsertInfo(const BoundCreateTableInfo &info, vector<LogicalType> &insert_types,
                                   vector<unique_ptr<Expression>> &bound_defaults) {
	auto &create_info = (CreateTableInfo &)*info.base;
	for (auto &col : create_info.columns.Physical()) {
		insert_types.push_back(col.GetType());
		bound_defaults.push_back(make_unique<BoundConstantExpression>(Value(col.GetType())));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalSinkState {
public:
	explicit InsertGlobalState(ClientContext &context, const vector<LogicalType> &return_types)
	    : insert_count(0), initialized(false), return_collection(context, return_types) {
	}

	mutex lock;
	DuckTableEntry *table;
	idx_t insert_count;
	bool initialized;
	LocalAppendState append_state;
	ColumnDataCollection return_collection;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                 const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(context, bound_defaults) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	TableAppendState local_append_state;
	unique_ptr<RowGroupCollection> local_collection;
	OptimisticDataWriter *writer;
	// Rows that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_rows;
};

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_unique<InsertGlobalState>(context, GetTypes());
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = *schema->catalog;
		result->table =
		    (DuckTableEntry *)catalog.CreateTable(catalog.GetCatalogTransaction(context), schema, info.get());
	} else {
		D_ASSERT(insert_table);
		D_ASSERT(insert_table->IsDuckTable());
		result->table = (DuckTableEntry *)insert_table;
	}
	return std::move(result);
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<InsertLocalState>(context.client, insert_types, bound_defaults);
}

void PhysicalInsert::ResolveDefaults(TableCatalogEntry *table, DataChunk &chunk,
                                     const physical_index_vector_t<idx_t> &column_index_map,
                                     ExpressionExecutor &default_executor, DataChunk &result) {
	chunk.Flatten();
	default_executor.SetChunk(chunk);

	result.Reset();
	result.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (auto &col : table->GetColumns().Physical()) {
			auto storage_idx = col.StorageOid();
			auto mapped_index = column_index_map[col.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// insert default value
				default_executor.ExecuteExpression(storage_idx, result.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)mapped_index < chunk.ColumnCount());
				D_ASSERT(result.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
				result.data[storage_idx].Reference(chunk.data[mapped_index]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < result.ColumnCount(); i++) {
			D_ASSERT(result.data[i].GetType() == chunk.data[i].GetType());
			result.data[i].Reference(chunk.data[i]);
		}
	}
}

bool AllConflictsMeetCondition(DataChunk &result) {
	auto data = FlatVector::GetData<bool>(result.data[0]);
	for (idx_t i = 0; i < result.size(); i++) {
		if (!data[i]) {
			return false;
		}
	}
	return true;
}

void CheckOnConflictCondition(ExecutionContext &context, DataChunk &conflicts, const unique_ptr<Expression> &condition,
                              DataChunk &result) {
	ExpressionExecutor executor(context.client, *condition);
	result.Initialize(context.client, {LogicalType::BOOLEAN});
	executor.Execute(conflicts, result);
	result.SetCardinality(conflicts.size());
}

void PhysicalInsert::CombineExistingAndInsertTuples(DataChunk &result, DataChunk &scan_chunk, DataChunk &input_chunk,
                                                    ClientContext &client) const {
	if (types_to_fetch.empty()) {
		// We have not scanned the initial table, so we can just duplicate the initial chunk
		result.Initialize(client, input_chunk.GetTypes());
		result.Reference(input_chunk);
		result.SetCardinality(input_chunk);
		return;
	}
	vector<LogicalType> combined_types;
	combined_types.reserve(insert_types.size() + types_to_fetch.size());
	combined_types.insert(combined_types.end(), insert_types.begin(), insert_types.end());
	combined_types.insert(combined_types.end(), types_to_fetch.begin(), types_to_fetch.end());

	result.Initialize(client, combined_types);
	result.Reset();
	// Add the VALUES list
	for (idx_t i = 0; i < insert_types.size(); i++) {
		idx_t col_idx = i;
		auto &other_col = input_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// Add the columns from the original conflicting tuples
	for (idx_t i = 0; i < types_to_fetch.size(); i++) {
		idx_t col_idx = i + insert_types.size();
		auto &other_col = scan_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// This is guaranteed by the requirement of a conflict target to have a condition or set expressions
	// Only when we have any sort of condition or SET expression that references the existing table is this possible
	// to not be true.
	// We can have a SET expression without a conflict target ONLY if there is only 1 Index on the table
	// In which case this also can't cause a discrepancy between existing tuple count and insert tuple count
	D_ASSERT(input_chunk.size() == scan_chunk.size());
	result.SetCardinality(input_chunk.size());
}

void PhysicalInsert::PerformOnConflictAction(ExecutionContext &context, DataChunk &chunk, TableCatalogEntry *table,
                                             Vector &row_ids) const {
	if (action_type == OnConflictAction::NOTHING) {
		return;
	}

	DataChunk update_chunk; // contains only the to-update columns

	// Check the optional condition for the DO UPDATE clause, to filter which rows will be updated
	if (do_update_condition) {
		DataChunk do_update_filter_result;
		do_update_filter_result.Initialize(context.client, {LogicalType::BOOLEAN});
		ExpressionExecutor where_executor(context.client, *do_update_condition);
		where_executor.Execute(chunk, do_update_filter_result);
		do_update_filter_result.SetCardinality(chunk.size());

		ManagedSelection selection(chunk.size());

		auto where_data = FlatVector::GetData<bool>(do_update_filter_result.data[0]);
		for (idx_t i = 0; i < chunk.size(); i++) {
			if (where_data[i]) {
				selection.Append(i);
			}
		}
		if (selection.Count() != selection.Size()) {
			// Not all conflicts met the condition, need to filter out the ones that don't
			chunk.Slice(selection.Selection(), selection.Count());
			chunk.SetCardinality(selection.Count());
			// Also apply this Slice to the to-update row_ids
			row_ids.Slice(selection.Selection(), selection.Count());
		}
	}

	// Execute the SET expressions
	update_chunk.Initialize(context.client, set_types);
	ExpressionExecutor executor(context.client, set_expressions);
	executor.Execute(chunk, update_chunk);
	update_chunk.SetCardinality(chunk);

	auto &data_table = table->GetStorage();
	// Perform the update, using the results of the SET expressions
	data_table.Update(*table, context.client, row_ids, set_columns, update_chunk);
}

// TODO: should we use a hash table to keep track of this instead?
void PhysicalInsert::RegisterUpdatedRows(InsertLocalState &lstate, const Vector &row_ids, idx_t count) const {
	// Insert all rows, if any of the rows has already been updated before, we throw an error
	auto data = FlatVector::GetData<row_t>(row_ids);
	for (idx_t i = 0; i < count; i++) {
		auto result = lstate.updated_rows.insert(data[i]);
		if (result.second == false) {
			throw InvalidInputException(
			    "ON CONFLICT DO UPDATE can not update the same row twice in the same command, Ensure that no rows "
			    "proposed for insertion within the same command have duplicate constrained values");
		}
	}
}

void PhysicalInsert::OnConflictHandling(TableCatalogEntry *table, ExecutionContext &context,
                                        InsertLocalState &lstate) const {
	auto &data_table = table->GetStorage();
	if (action_type == OnConflictAction::THROW) {
		data_table.VerifyAppendConstraints(*table, context.client, lstate.insert_chunk, nullptr);
		return;
	}
	// Check whether any conflicts arise, and if they all meet the conflict_target + condition
	// If that's not the case - We throw the first error

	// We either want to do nothing, or perform an update when conflicts arise
	ConflictInfo conflict_info(conflict_target);
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, lstate.insert_chunk.size(), &conflict_info);
	data_table.VerifyAppendConstraints(*table, context.client, lstate.insert_chunk, &conflict_manager);
	conflict_manager.Finalize();
	if (conflict_manager.ConflictCount() == 0) {
		// No conflicts found
		return;
	}
	auto &conflicts = conflict_manager.Conflicts();
	auto &row_ids = conflict_manager.RowIds();

	DataChunk conflict_chunk; // contains only the conflicting values
	DataChunk scan_chunk;     // contains the original values, that caused the conflict
	DataChunk combined_chunk; // contains conflict_chunk + scan_chunk (wide)

	// Filter out everything but the conflicting rows
	conflict_chunk.Initialize(context.client, lstate.insert_chunk.GetTypes());
	conflict_chunk.Reference(lstate.insert_chunk);
	conflict_chunk.Slice(conflicts.Selection(), conflicts.Count());
	conflict_chunk.SetCardinality(conflicts.Count());

	if (!types_to_fetch.empty()) {
		D_ASSERT(scan_chunk.size() == 0);
		// When these values are required for the conditions or the SET expressions,
		// then we scan the existing table for the conflicting tuples, using the rowids
		scan_chunk.Initialize(context.client, types_to_fetch);
		auto fetch_state = make_unique<ColumnFetchState>();
		auto &transaction = DuckTransaction::Get(context.client, *table->catalog);
		data_table.Fetch(transaction, scan_chunk, columns_to_fetch, row_ids, conflicts.Count(), *fetch_state);
	}

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context.client);

	if (on_conflict_condition) {
		DataChunk conflict_condition_result;
		CheckOnConflictCondition(context, combined_chunk, on_conflict_condition, conflict_condition_result);
		bool conditions_met = AllConflictsMeetCondition(conflict_condition_result);
		if (!conditions_met) {
			// Filter out the tuples that did pass the filter, then run the verify again
			ManagedSelection sel(combined_chunk.size());
			auto data = FlatVector::GetData<bool>(conflict_condition_result.data[0]);
			for (idx_t i = 0; i < combined_chunk.size(); i++) {
				if (!data[i]) {
					// Only populate the selection vector with the tuples that did not meet the condition
					sel.Append(i);
				}
			}
			combined_chunk.Slice(sel.Selection(), sel.Count());
			row_ids.Slice(sel.Selection(), sel.Count());
			data_table.VerifyAppendConstraints(*table, context.client, combined_chunk, nullptr);
			throw InternalException("The previous operation was expected to throw but didn't");
		}
	}

	RegisterUpdatedRows(lstate, row_ids, combined_chunk.size());

	PerformOnConflictAction(context, combined_chunk, table, row_ids);

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(lstate.insert_chunk.size());
	idx_t new_size =
	    SelectionVector::Inverted(conflicts.Selection(), sel_vec, conflicts.Count(), lstate.insert_chunk.size());
	lstate.insert_chunk.Slice(sel_vec, new_size);
	lstate.insert_chunk.SetCardinality(new_size);
}

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                    DataChunk &chunk) const {
	auto &gstate = (InsertGlobalState &)state;
	auto &lstate = (InsertLocalState &)lstate_p;

	auto table = gstate.table;
	auto &storage = table->GetStorage();
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!parallel) {
		if (!gstate.initialized) {
			storage.InitializeLocalAppend(gstate.append_state, context.client);
			gstate.initialized = true;
		}

		OnConflictHandling(table, context, lstate);
		storage.LocalAppend(gstate.append_state, *table, context.client, lstate.insert_chunk, true);

		if (return_chunk) {
			gstate.return_collection.Append(lstate.insert_chunk);
		}
		gstate.insert_count += chunk.size();
	} else {
		D_ASSERT(!return_chunk);
		// parallel append
		if (!lstate.local_collection) {
			lock_guard<mutex> l(gstate.lock);
			auto &table_info = storage.info;
			auto &block_manager = TableIOManager::Get(storage).GetBlockManagerForRowData();
			lstate.local_collection =
			    make_unique<RowGroupCollection>(table_info, block_manager, insert_types, MAX_ROW_ID);
			lstate.local_collection->InitializeEmpty();
			lstate.local_collection->InitializeAppend(lstate.local_append_state);
			lstate.writer = gstate.table->GetStorage().CreateOptimisticWriter(context.client);
		}
		OnConflictHandling(table, context, lstate);
		auto new_row_group = lstate.local_collection->Append(lstate.insert_chunk, lstate.local_append_state);
		if (new_row_group) {
			lstate.writer->CheckFlushToDisk(*lstate.local_collection);
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (InsertGlobalState &)gstate_p;
	auto &lstate = (InsertLocalState &)lstate_p;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	if (!parallel) {
		return;
	}
	if (!lstate.local_collection) {
		return;
	}
	// parallel append: finalize the append
	TransactionData tdata(0, 0);
	lstate.local_collection->FinalizeAppend(tdata, lstate.local_append_state);

	auto append_count = lstate.local_collection->GetTotalRows();

	if (append_count < LocalStorage::MERGE_THRESHOLD) {
		// we have few rows - append to the local storage directly
		lock_guard<mutex> lock(gstate.lock);
		gstate.insert_count += append_count;
		auto table = gstate.table;
		auto &storage = table->GetStorage();
		storage.InitializeLocalAppend(gstate.append_state, context.client);
		auto &transaction = DuckTransaction::Get(context.client, *table->catalog);
		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
			storage.LocalAppend(gstate.append_state, *table, context.client, insert_chunk);
			return true;
		});
		storage.FinalizeLocalAppend(gstate.append_state);
	} else {
		// we have many rows - flush the row group collection to disk (if required) and merge into the transaction-local
		// state
		lstate.writer->FlushToDisk(*lstate.local_collection);
		lstate.writer->FinalFlush();

		lock_guard<mutex> lock(gstate.lock);
		gstate.insert_count += append_count;
		gstate.table->GetStorage().LocalMerge(context.client, *lstate.local_collection);
	}
}

SinkFinalizeType PhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &state) const {
	auto &gstate = (InsertGlobalState &)state;
	if (!parallel && gstate.initialized) {
		auto table = gstate.table;
		auto &storage = table->GetStorage();
		storage.FinalizeLocalAppend(gstate.append_state);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class InsertSourceState : public GlobalSourceState {
public:
	explicit InsertSourceState(const PhysicalInsert &op) : finished(false) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = (InsertGlobalState &)*op.sink_state;
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<InsertSourceState>(*this);
}

void PhysicalInsert::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (InsertSourceState &)gstate;
	auto &insert_gstate = (InsertGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
		state.finished = true;
		return;
	}

	insert_gstate.return_collection.Scan(state.scan_state, chunk);
}

} // namespace duckdb
