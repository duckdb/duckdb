#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalBatchInsert::PhysicalBatchInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
                               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(table), insert_types(table->GetTypes()),
      bound_defaults(move(bound_defaults)) {
}

PhysicalBatchInsert::PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      schema(schema), info(move(info_p)) {
	PhysicalInsert::GetInsertInfo(*info, insert_types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchInsertGlobalState : public GlobalSinkState {
public:
	explicit BatchInsertGlobalState()
	    : insert_count(0) {
	}

	mutex lock;
	TableCatalogEntry *table;
	idx_t insert_count;
	map<idx_t, unique_ptr<RowGroupCollection>> collections;

	void AddCollection(idx_t batch_index, unique_ptr<RowGroupCollection> current_collection) {
		lock_guard<mutex> l(lock);
		insert_count += current_collection->GetTotalRows();
		collections[batch_index] = move(current_collection);
	}
};

class BatchInsertLocalState : public LocalSinkState {
public:
	BatchInsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                 const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(Allocator::Get(context), bound_defaults) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	idx_t current_index;
	TableAppendState current_append_state;
	unique_ptr<RowGroupCollection> current_collection;
//	unique_ptr<OptimisticDataWriter> writer;

	void CreateNewCollection(TableCatalogEntry *table, const vector<LogicalType> &insert_types) {
		auto &table_info = table->storage->info;
		auto &block_manager = TableIOManager::Get(*table->storage).GetBlockManagerForRowData();
		current_collection = make_unique<RowGroupCollection>(table_info, block_manager, insert_types, 0);
		current_collection->InitializeEmpty();
		current_collection->InitializeAppend(current_append_state);
	}
};

unique_ptr<GlobalSinkState> PhysicalBatchInsert::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_unique<BatchInsertGlobalState>();
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = Catalog::GetCatalog(context);
		result->table = (TableCatalogEntry *)catalog.CreateTable(context, schema, info.get());
	} else {
		D_ASSERT(insert_table);
		result->table = insert_table;
	}
	return move(result);
}

unique_ptr<LocalSinkState> PhysicalBatchInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BatchInsertLocalState>(context.client, insert_types, bound_defaults);
}

SinkResultType PhysicalBatchInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
					DataChunk &chunk) const {
	auto &gstate = (BatchInsertGlobalState &)state;
	auto &lstate = (BatchInsertLocalState &)lstate_p;

	auto table = gstate.table;
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!lstate.current_collection) {
		// no collection yet: create a new one
		lstate.CreateNewCollection(table, insert_types);
//		lstate.writer = make_unique<OptimisticDataWriter>(gstate.table->storage.get());
	} else if (lstate.current_index != lstate.batch_index) {
		// batch index has changed: move the old collection to the global state and create a new collection
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		gstate.AddCollection(lstate.current_index, move(lstate.current_collection));
		lstate.CreateNewCollection(table, insert_types);
	}
	lstate.current_index = lstate.batch_index;
	auto new_row_group = lstate.current_collection->Append(lstate.insert_chunk, lstate.current_append_state);
//	if (new_row_group) {
//		lstate.writer->CheckFlushToDisk(*lstate.current_collection);
//	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;
	auto &lstate = (BatchInsertLocalState &)lstate_p;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	if (!lstate.current_collection) {
		return;
	}
//	lstate.writer->FlushToDisk(*lstate.local_collection);
//	lstate.writer->FinalFlush();
	TransactionData tdata(0, 0);
	lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
	gstate.AddCollection(lstate.current_index, move(lstate.current_collection));
//
//	if (!parallel) {
//		return;
//	}
//	if (!lstate.local_collection) {
//		return;
//	}
//	// parallel append: finalize the append
//	TransactionData tdata(0, 0);
//	lstate.local_collection->FinalizeAppend(tdata, lstate.local_append_state);
//
//	auto append_count = lstate.local_collection->GetTotalRows();
//
//	if (append_count < LocalStorage::MERGE_THRESHOLD) {
//		// we have few rows - append to the local storage directly
//		lock_guard<mutex> lock(gstate.lock);
//		gstate.insert_count += append_count;
//		auto table = gstate.table;
//		table->storage->InitializeLocalAppend(gstate.append_state, context.client);
//		auto &transaction = Transaction::GetTransaction(context.client);
//		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
//			table->storage->LocalAppend(gstate.append_state, *table, context.client, insert_chunk);
//			return true;
//		});
//		table->storage->FinalizeLocalAppend(gstate.append_state);
//	} else {
//		// we have many rows - flush the row group collection to disk (if required) and merge into the transaction-local
//		// state
//		lstate.writer->FlushToDisk(*lstate.local_collection);
//		lstate.writer->FinalFlush();
//
//		lock_guard<mutex> lock(gstate.lock);
//		gstate.insert_count += append_count;
//		gstate.table->storage->LocalMerge(context.client, *lstate.local_collection);
//	}
}

SinkFinalizeType PhysicalBatchInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
						  GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;
	for(auto &collection : gstate.collections) {
		gstate.table->storage->LocalMerge(context, *collection.second);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class BatchInsertSourceState : public GlobalSourceState {
public:
	explicit BatchInsertSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalBatchInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<BatchInsertSourceState>();
}

void PhysicalBatchInsert::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (BatchInsertSourceState &)gstate;
	auto &insert_gstate = (BatchInsertGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
	state.finished = true;
	return;
}

}
