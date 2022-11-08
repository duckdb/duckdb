#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

PhysicalInsert::PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
                               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality,
                               bool return_chunk, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(table), insert_types(table->GetTypes()),
      bound_defaults(move(bound_defaults)), return_chunk(return_chunk), parallel(parallel) {
}

PhysicalInsert::PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      return_chunk(false), schema(schema), info(move(info_p)), parallel(parallel) {
	GetInsertInfo(*info, insert_types, bound_defaults);
}

void PhysicalInsert::GetInsertInfo(const BoundCreateTableInfo &info, vector<LogicalType> &insert_types,
                                   vector<unique_ptr<Expression>> &bound_defaults) {
	auto &create_info = (CreateTableInfo &)*info.base;
	for (auto &col : create_info.columns) {
		if (col.Generated()) {
			continue;
		}
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
	TableCatalogEntry *table;
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
};

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_unique<InsertGlobalState>(context, GetTypes());
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

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<InsertLocalState>(context.client, insert_types, bound_defaults);
}

void PhysicalInsert::ResolveDefaults(TableCatalogEntry *table, DataChunk &chunk, const vector<idx_t> &column_index_map,
                                     ExpressionExecutor &default_executor, DataChunk &result) {
	chunk.Flatten();
	default_executor.SetChunk(chunk);

	result.Reset();
	result.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (idx_t i = 0; i < table->columns.size(); i++) {
			auto &col = table->columns[i];
			if (col.Generated()) {
				continue;
			}
			auto storage_idx = col.StorageOid();
			if (column_index_map[i] == DConstants::INVALID_INDEX) {
				// insert default value
				default_executor.ExecuteExpression(i, result.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)column_index_map[i] < chunk.ColumnCount());
				D_ASSERT(result.data[storage_idx].GetType() == chunk.data[column_index_map[i]].GetType());
				result.data[storage_idx].Reference(chunk.data[column_index_map[i]]);
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

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                    DataChunk &chunk) const {
	auto &gstate = (InsertGlobalState &)state;
	auto &lstate = (InsertLocalState &)lstate_p;

	auto table = gstate.table;
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!parallel) {
		if (!gstate.initialized) {
			table->storage->InitializeLocalAppend(gstate.append_state, context.client);
			gstate.initialized = true;
		}
		table->storage->LocalAppend(gstate.append_state, *table, context.client, lstate.insert_chunk);

		if (return_chunk) {
			gstate.return_collection.Append(lstate.insert_chunk);
		}
		gstate.insert_count += chunk.size();
	} else {
		D_ASSERT(!return_chunk);
		// parallel append
		if (!lstate.local_collection) {
			lock_guard<mutex> l(gstate.lock);
			auto &table_info = table->storage->info;
			auto &block_manager = TableIOManager::Get(*table->storage).GetBlockManagerForRowData();
			lstate.local_collection =
			    make_unique<RowGroupCollection>(table_info, block_manager, insert_types, MAX_ROW_ID);
			lstate.local_collection->InitializeEmpty();
			lstate.local_collection->InitializeAppend(lstate.local_append_state);
			lstate.writer = gstate.table->storage->CreateOptimisticWriter(context.client);
		}
		table->storage->VerifyAppendConstraints(*table, context.client, lstate.insert_chunk);
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
		table->storage->InitializeLocalAppend(gstate.append_state, context.client);
		auto &transaction = Transaction::GetTransaction(context.client);
		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
			table->storage->LocalAppend(gstate.append_state, *table, context.client, insert_chunk);
			return true;
		});
		table->storage->FinalizeLocalAppend(gstate.append_state);
	} else {
		// we have many rows - flush the row group collection to disk (if required) and merge into the transaction-local
		// state
		lstate.writer->FlushToDisk(*lstate.local_collection);
		lstate.writer->FinalFlush();

		lock_guard<mutex> lock(gstate.lock);
		gstate.insert_count += append_count;
		gstate.table->storage->LocalMerge(context.client, *lstate.local_collection);
	}
}

SinkFinalizeType PhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &state) const {
	auto &gstate = (InsertGlobalState &)state;
	if (!parallel && gstate.initialized) {
		auto table = gstate.table;
		table->storage->FinalizeLocalAppend(gstate.append_state);
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
