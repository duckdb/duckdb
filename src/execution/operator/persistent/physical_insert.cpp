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
	InsertGlobalState(const vector<LogicalType> &types, const vector<unique_ptr<Expression>> &bound_defaults)
		: default_executor(bound_defaults) {
		insert_count = 0;
		returned_chunk_count = 0;
		return_chunk_collection = ChunkCollection();
		returning_types = types;
	}

	mutex lock;
	idx_t insert_count;
	ChunkCollection return_chunk_collection;
	vector<LogicalType> returning_types;
	idx_t returned_chunk_count;
	ExpressionExecutor default_executor;
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
                               vector<unique_ptr<Expression>> returning_values)
    : PhysicalOperator(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)),
      table(table),
      bound_defaults(move(bound_defaults)),
      returning_values(move(returning_values)) {
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
			if (column_index_map[i] == DConstants::INVALID_INDEX) {
				// insert default value
				istate.default_executor.ExecuteExpression(i, istate.insert_chunk.data[i]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)column_index_map[i] < chunk.ColumnCount());
				D_ASSERT(istate.insert_chunk.data[i].GetType() == chunk.data[column_index_map[i]].GetType());
				istate.insert_chunk.data[i].Reference(chunk.data[column_index_map[i]]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < istate.insert_chunk.ColumnCount(); i++) {
			D_ASSERT(istate.insert_chunk.data[i].GetType() == chunk.data[i].GetType());
			istate.insert_chunk.data[i].Reference(chunk.data[i]);
		}
	}

	DataChunk returning_chunk;
	if (!returning_values.empty()) {
		auto return_executor = ExpressionExecutor(returning_values);
		returning_chunk.Initialize(gstate.returning_types);
		return_executor.Execute(istate.insert_chunk, returning_chunk);
	}

	lock_guard<mutex> glock(gstate.lock);
	table->storage->Append(*table, context.client, istate.insert_chunk);

	if (!returning_values.empty()) {
		gstate.return_chunk_collection.Append(returning_chunk);
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	vector<LogicalType> returning_types;
	for (idx_t i = 0; i < returning_values.size(); i++) {
		returning_types.push_back(returning_values[i]->return_type);
	}
	return make_unique<InsertGlobalState>(returning_types, bound_defaults);
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
	if (insert_gstate.returning_types.empty()) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
		state.finished = true;
	} else {
		idx_t chunk_return = insert_gstate.returned_chunk_count;
		(insert_gstate.return_chunk_collection.Chunks().at(chunk_return))->Copy(chunk);
		chunk.SetCardinality((insert_gstate.return_chunk_collection.Chunks().at(chunk_return))->size());
		insert_gstate.returned_chunk_count += 1;
		if (insert_gstate.returned_chunk_count >= insert_gstate.return_chunk_collection.Chunks().size()) {
			state.finished = true;
		}
	}
}

} // namespace duckdb
