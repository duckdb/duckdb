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
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_state.hpp"

namespace duckdb {

PhysicalInsert::PhysicalInsert(
    vector<LogicalType> types_p, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map,
    vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints_p,
    vector<unique_ptr<Expression>> set_expressions, vector<PhysicalIndex> set_columns, vector<LogicalType> set_types,
    idx_t estimated_cardinality, bool return_chunk, bool parallel, OnConflictAction action_type,
    unique_ptr<Expression> on_conflict_condition_p, unique_ptr<Expression> do_update_condition_p,
    unordered_set<column_t> conflict_target_p, vector<column_t> columns_to_fetch_p)
    : PhysicalOperator(PhysicalOperatorType::INSERT, std::move(types_p), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(&table), insert_types(table.GetTypes()),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints_p)),
      return_chunk(return_chunk), parallel(parallel), action_type(action_type),
      set_expressions(std::move(set_expressions)), set_columns(std::move(set_columns)), set_types(std::move(set_types)),
      on_conflict_condition(std::move(on_conflict_condition_p)), do_update_condition(std::move(do_update_condition_p)),
      conflict_target(std::move(conflict_target_p)), columns_to_fetch(std::move(columns_to_fetch_p)) {

	if (action_type == OnConflictAction::THROW) {
		return;
	}

	D_ASSERT(this->set_expressions.size() == this->set_columns.size());

	// One or more columns are referenced from the existing table,
	// we use the 'insert_types' to figure out which types these columns have
	types_to_fetch = vector<LogicalType>(columns_to_fetch.size(), LogicalType::SQLNULL);
	for (idx_t i = 0; i < columns_to_fetch.size(); i++) {
		auto &id = columns_to_fetch[i];
		D_ASSERT(id < insert_types.size());
		types_to_fetch[i] = insert_types[id];
	}
}

PhysicalInsert::PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      return_chunk(false), schema(&schema), info(std::move(info_p)), parallel(parallel),
      action_type(OnConflictAction::THROW) {
	GetInsertInfo(*info, insert_types, bound_defaults);
}

void PhysicalInsert::GetInsertInfo(const BoundCreateTableInfo &info, vector<LogicalType> &insert_types,
                                   vector<unique_ptr<Expression>> &bound_defaults) {
	auto &create_info = info.base->Cast<CreateTableInfo>();
	for (auto &col : create_info.columns.Physical()) {
		insert_types.push_back(col.GetType());
		bound_defaults.push_back(make_uniq<BoundConstantExpression>(Value(col.GetType())));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalSinkState {
public:
	explicit InsertGlobalState(ClientContext &context, const vector<LogicalType> &return_types, DuckTableEntry &table)
	    : table(table), insert_count(0), initialized(false), return_collection(context, return_types) {
	}

	mutex lock;
	DuckTableEntry &table;
	idx_t insert_count;
	bool initialized;
	LocalAppendState append_state;
	ColumnDataCollection return_collection;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                 const vector<unique_ptr<Expression>> &bound_defaults,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints)
	    : default_executor(context, bound_defaults), bound_constraints(bound_constraints) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	TableAppendState local_append_state;
	unique_ptr<RowGroupCollection> local_collection;
	optional_ptr<OptimisticDataWriter> writer;
	// Rows that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_global_rows;
	// Rows in the transaction-local storage that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_local_rows;
	idx_t update_count = 0;
	unique_ptr<ConstraintState> constraint_state;
	const vector<unique_ptr<BoundConstraint>> &bound_constraints;

	ConstraintState &GetConstraintState(DataTable &table, TableCatalogEntry &tableref) {
		if (!constraint_state) {
			constraint_state = table.InitializeConstraintState(tableref, bound_constraints);
		}
		return *constraint_state;
	}
};

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<TableCatalogEntry> table;
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = schema->catalog;
		table = &catalog.CreateTable(catalog.GetCatalogTransaction(context), *schema.get_mutable(), *info)
		             ->Cast<TableCatalogEntry>();
	} else {
		D_ASSERT(insert_table);
		D_ASSERT(insert_table->IsDuckTable());
		table = insert_table.get_mutable();
	}
	auto result = make_uniq<InsertGlobalState>(context, GetTypes(), table->Cast<DuckTableEntry>());
	return std::move(result);
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<InsertLocalState>(context.client, insert_types, bound_defaults, bound_constraints);
}

void PhysicalInsert::ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
                                     const physical_index_vector_t<idx_t> &column_index_map,
                                     ExpressionExecutor &default_executor, DataChunk &result) {
	chunk.Flatten();
	default_executor.SetChunk(chunk);

	result.Reset();
	result.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (auto &col : table.GetColumns().Physical()) {
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

idx_t PhysicalInsert::OnConflictHandling(DataTable &storage, TableCatalogEntry &table, ExecutionContext &context,
                                         InsertLocalState &lstate, InsertGlobalState &gstate, bool do_inserts) const {

	idx_t update_count = 0, insert_count = 0;
	auto &data_table = table.GetStorage();
	auto throw_on_conflict = action_type == OnConflictAction::THROW;

	if (action_type != OnConflictAction::NOTHING && return_chunk) {
		// If the action is UPDATE or REPLACE, we will always create either an APPEND or an INSERT
		// for NOTHING we don't create either an APPEND or an INSERT for the tuple
		// so it should not be added to the RETURNING chunk
		gstate.return_collection.Append(lstate.insert_chunk);
	}

	if (throw_on_conflict) {
		auto &constraint_state = lstate.GetConstraintState(data_table, table);
		data_table.VerifyAppendConstraints(constraint_state, context.client, lstate.insert_chunk, nullptr);
	}

	if (do_inserts && throw_on_conflict) {
		if (!gstate.initialized) {
			storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
			gstate.initialized = true;
		}

		storage.LocalAppend(gstate.append_state, table, context.client, lstate.insert_chunk, true);
		insert_count = lstate.insert_chunk.size();

	} else if (!throw_on_conflict) {
		if (!gstate.initialized) {
			storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
			gstate.initialized = true;
		}

		storage.Merge(table, context.client, lstate.insert_chunk, bound_constraints,
		                   conflict_target, set_columns, gstate.append_state, true,
		                   do_inserts, update_count, insert_count, types_to_fetch, insert_types, on_conflict_condition,
		                   columns_to_fetch, set_expressions, set_types, do_update_condition);

		gstate.initialized = false;
	}

	if (action_type == OnConflictAction::NOTHING && return_chunk) {
		// Because we didn't add to the RETURNING chunk yet
		// we add the tuples that did not get filtered out now
		gstate.return_collection.Append(lstate.insert_chunk);
	}

	return update_count + insert_count;
}

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	auto &lstate = input.local_state.Cast<InsertLocalState>();

	auto &table = gstate.table;
	auto &storage = table.GetStorage();
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!parallel) {
		if (!gstate.initialized) {
			storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
			gstate.initialized = true;
		}
		auto changes = OnConflictHandling(storage, table, context, lstate, gstate, true);
		gstate.insert_count += changes;
	} else {
		D_ASSERT(!return_chunk);
		// parallel append
		if (!lstate.local_collection) {
			lock_guard<mutex> l(gstate.lock);
			auto table_info = storage.GetDataTableInfo();
			auto &block_manager = TableIOManager::Get(storage).GetBlockManagerForRowData();
			lstate.local_collection = make_uniq<RowGroupCollection>(std::move(table_info), block_manager, insert_types,
			                                                        NumericCast<idx_t>(MAX_ROW_ID));
			lstate.local_collection->InitializeEmpty();
			lstate.local_collection->InitializeAppend(lstate.local_append_state);
			lstate.writer = &gstate.table.GetStorage().CreateOptimisticWriter(context.client);
		}
		OnConflictHandling(storage, table, context, lstate, gstate, false);

		auto new_row_group = lstate.local_collection->Append(lstate.insert_chunk, lstate.local_append_state);
		if (new_row_group) {
			lstate.writer->WriteNewRowGroup(*lstate.local_collection);
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	auto &lstate = input.local_state.Cast<InsertLocalState>();
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);

	if (!parallel || !lstate.local_collection) {
		return SinkCombineResultType::FINISHED;
	}

	// parallel append: finalize the append
	TransactionData tdata(0, 0);
	lstate.local_collection->FinalizeAppend(tdata, lstate.local_append_state);

	auto append_count = lstate.local_collection->GetTotalRows();

	lock_guard<mutex> lock(gstate.lock);
	gstate.insert_count += append_count;
	if (append_count < Storage::ROW_GROUP_SIZE) {
		// we have few rows - append to the local storage directly
		auto &table = gstate.table;
		auto &storage = table.GetStorage();
		storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
		auto &transaction = DuckTransaction::Get(context.client, table.catalog);
		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
			storage.LocalAppend(gstate.append_state, table, context.client, insert_chunk);
			return true;
		});
		storage.FinalizeLocalAppend(gstate.append_state);
	} else {
		// we have written rows to disk optimistically - merge directly into the transaction-local storage
		gstate.table.GetStorage().LocalMerge(context.client, *lstate.local_collection);
		gstate.table.GetStorage().FinalizeOptimisticWriter(context.client, *lstate.writer);
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	if (!parallel && gstate.initialized) {
		auto &table = gstate.table;
		auto &storage = table.GetStorage();
		storage.FinalizeLocalAppend(gstate.append_state);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class InsertSourceState : public GlobalSourceState {
public:
	explicit InsertSourceState(const PhysicalInsert &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<InsertGlobalState>();
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<InsertSourceState>(*this);
}

SourceResultType PhysicalInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<InsertSourceState>();
	auto &insert_gstate = sink_state->Cast<InsertGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
		return SourceResultType::FINISHED;
	}

	insert_gstate.return_collection.Scan(state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
