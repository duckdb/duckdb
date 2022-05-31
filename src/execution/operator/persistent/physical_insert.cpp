#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalSinkState {
public:
	InsertGlobalState() : insert_count(0), returned_chunk_count(0) {
	}

	mutex lock;
	idx_t insert_count;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(const vector<LogicalType> &types, const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(bound_defaults) {
		insert_chunk.Initialize(types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
};

PhysicalInsert::PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
                               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality,
                               bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), table(table), bound_defaults(move(bound_defaults)),
      return_chunk(return_chunk) {
}

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &chunk) const {
	auto &gstate = (InsertGlobalState &)state;
	auto &istate = (InsertLocalState &)lstate;

	chunk.Normalify();
	istate.default_executor.SetChunk(chunk);

	istate.insert_chunk.Reset();
	istate.insert_chunk.SetCardinality(chunk);

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
				istate.default_executor.ExecuteExpression(i, istate.insert_chunk.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)column_index_map[i] < chunk.ColumnCount());
				D_ASSERT(istate.insert_chunk.data[storage_idx].GetType() == chunk.data[column_index_map[i]].GetType());
				istate.insert_chunk.data[storage_idx].Reference(chunk.data[column_index_map[i]]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < istate.insert_chunk.ColumnCount(); i++) {
			D_ASSERT(istate.insert_chunk.data[i].GetType() == chunk.data[i].GetType());
			istate.insert_chunk.data[i].Reference(chunk.data[i]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	table->storage->Append(*table, context.client, istate.insert_chunk);

	if (return_chunk) {
		gstate.return_chunk_collection.Append(istate.insert_chunk);
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<InsertGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<InsertLocalState>(table->GetTypes(), bound_defaults);
}

void PhysicalInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (InsertLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class InsertSourceState : public GlobalSourceState {
public:
	InsertSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<InsertSourceState>();
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
	}

	idx_t chunk_return = insert_gstate.returned_chunk_count;
	if (chunk_return >= insert_gstate.return_chunk_collection.Chunks().size()) {
		return;
	}

	chunk.Reference(insert_gstate.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((insert_gstate.return_chunk_collection.GetChunk(chunk_return)).size());
	insert_gstate.returned_chunk_count += 1;
	if (insert_gstate.returned_chunk_count >= insert_gstate.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb
