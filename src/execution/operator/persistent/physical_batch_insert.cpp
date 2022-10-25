#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalBatchInsert::PhysicalBatchInsert(vector<LogicalType> types, TableCatalogEntry *table,
                                         vector<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(table), insert_types(table->GetTypes()),
      bound_defaults(move(bound_defaults)) {
}

PhysicalBatchInsert::PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_CREATE_TABLE_AS, op.types, estimated_cardinality),
      insert_table(nullptr), schema(schema), info(move(info_p)) {
	PhysicalInsert::GetInsertInfo(*info, insert_types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchInsertGlobalState : public GlobalSinkState {
public:
	explicit BatchInsertGlobalState() : insert_count(0) {
	}

	mutex lock;
	TableCatalogEntry *table;
	idx_t insert_count;
	map<idx_t, unique_ptr<RowGroupCollection>> collections;

	void AddCollection(idx_t batch_index, unique_ptr<RowGroupCollection> current_collection) {
		lock_guard<mutex> l(lock);
		insert_count += current_collection->GetTotalRows();
		if (collections.find(batch_index) != collections.end()) {
			throw InternalException("PhysicalBatchInsert::AddCollection error: batch index %d is present in multiple "
			                        "collections. This occurs when "
			                        "batch indexes are not uniquely distributed over threads",
			                        batch_index);
		}
		collections[batch_index] = move(current_collection);
	}
};

class BatchInsertLocalState : public LocalSinkState {
public:
	BatchInsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                      const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(Allocator::Get(context), bound_defaults), written_to_disk(false) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	idx_t current_index;
	TableAppendState current_append_state;
	unique_ptr<RowGroupCollection> current_collection;
	unique_ptr<OptimisticDataWriter> writer;
	bool written_to_disk;

	void CreateNewCollection(TableCatalogEntry *table, const vector<LogicalType> &insert_types) {
		auto &table_info = table->storage->info;
		auto &block_manager = TableIOManager::Get(*table->storage).GetBlockManagerForRowData();
		current_collection = make_unique<RowGroupCollection>(table_info, block_manager, insert_types, MAX_ROW_ID);
		current_collection->InitializeEmpty();
		current_collection->InitializeAppend(current_append_state);
		written_to_disk = false;
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
		lstate.writer = make_unique<OptimisticDataWriter>(gstate.table->storage.get());
	} else if (lstate.current_index != lstate.batch_index) {
		// batch index has changed: move the old collection to the global state and create a new collection
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		if (lstate.written_to_disk) {
			lstate.writer->FlushToDisk(*lstate.current_collection);
		}
		gstate.AddCollection(lstate.current_index, move(lstate.current_collection));
		lstate.CreateNewCollection(table, insert_types);
	}
	lstate.current_index = lstate.batch_index;
	table->storage->VerifyAppendConstraints(*table, context.client, lstate.insert_chunk);
	auto new_row_group = lstate.current_collection->Append(lstate.insert_chunk, lstate.current_append_state);
	if (new_row_group) {
		lstate.writer->CheckFlushToDisk(*lstate.current_collection);
		lstate.written_to_disk = true;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;
	auto &lstate = (BatchInsertLocalState &)lstate_p;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	if (!lstate.current_collection) {
		return;
	}
	if (lstate.written_to_disk) {
		lstate.writer->FlushToDisk(*lstate.current_collection);
	}
	lstate.writer->FinalFlush();

	TransactionData tdata(0, 0);
	lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
	gstate.AddCollection(lstate.current_index, move(lstate.current_collection));
}

struct CollectionMerger {
	vector<unique_ptr<RowGroupCollection>> current_collections;

	void AddCollection(unique_ptr<RowGroupCollection> collection) {
		current_collections.push_back(move(collection));
	}

	bool Empty() {
		return current_collections.empty();
	}

	void Flush(ClientContext &context, DataTable &storage) {
		if (Empty()) {
			return;
		}
		unique_ptr<RowGroupCollection> new_collection;
		if (current_collections.size() == 1) {
			// we have gathered only one row group collection: merge it directly
			new_collection = move(current_collections[0]);
		} else {
			// we have gathered multiple collections: create one big collection and merge that
			auto &table_info = storage.info;
			auto &block_manager = TableIOManager::Get(storage).GetBlockManagerForRowData();
			auto types = storage.GetTypes();
			new_collection = make_unique<RowGroupCollection>(table_info, block_manager, types, MAX_ROW_ID);
			TableAppendState append_state;
			new_collection->InitializeEmpty();
			new_collection->InitializeAppend(append_state);

			DataChunk scan_chunk;
			scan_chunk.Initialize(context, types);

			vector<column_t> column_ids;
			for (idx_t i = 0; i < types.size(); i++) {
				column_ids.push_back(i);
			}
			for (auto &collection : current_collections) {
				TableScanState scan_state;
				scan_state.Initialize(column_ids);
				collection->InitializeScan(scan_state.local_state, column_ids, nullptr);

				while (true) {
					scan_chunk.Reset();
					scan_state.local_state.ScanCommitted(scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
					if (scan_chunk.size() == 0) {
						break;
					}
					new_collection->Append(scan_chunk, append_state);
				}
			}

			new_collection->FinalizeAppend(TransactionData(0, 0), append_state);
		}
		storage.LocalMerge(context, *new_collection);
		current_collections.clear();
	}
};

SinkFinalizeType PhysicalBatchInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;

	CollectionMerger merger;

	auto &storage = *gstate.table->storage;
	for (auto &collection : gstate.collections) {
		if (collection.second->GetTotalRows() < LocalStorage::MERGE_THRESHOLD) {
			// this collection has very few rows: add it to the merge set
			merger.AddCollection(move(collection.second));
		} else {
			if (!merger.Empty()) {
				// we have small collections remaining: flush them
				merger.Flush(context, storage);
			}
			storage.LocalMerge(context, *collection.second);
		}
	}
	merger.Flush(context, storage);
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

} // namespace duckdb
