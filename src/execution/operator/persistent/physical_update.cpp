#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class UpdateGlobalState : public GlobalSinkState {
public:
	explicit UpdateGlobalState(Allocator &allocator)
	    : updated_count(0), return_chunk_collection(allocator), returned_chunk_count(0) {
	}

	mutex lock;
	idx_t updated_count;
	unordered_set<row_t> updated_columns;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(Allocator &allocator, const vector<unique_ptr<Expression>> &expressions,
	                 const vector<LogicalType> &table_types, const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(allocator, bound_defaults) {
		// initialize the update chunk
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->return_type);
		}
		update_chunk.Initialize(allocator, update_types);
		// initialize the mock chunk
		mock_chunk.Initialize(allocator, table_types);
	}

	DataChunk update_chunk;
	DataChunk mock_chunk;
	ExpressionExecutor default_executor;
};

SinkResultType PhysicalUpdate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &chunk) const {
	auto &gstate = (UpdateGlobalState &)state;
	auto &ustate = (UpdateLocalState &)lstate;

	DataChunk &update_chunk = ustate.update_chunk;
	DataChunk &mock_chunk = ustate.mock_chunk;

	chunk.Flatten();
	ustate.default_executor.SetChunk(chunk);

	// update data in the base table
	// the row ids are given to us as the last column of the child chunk
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	update_chunk.SetCardinality(chunk);
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
			// default expression, set to the default value of the column
			ustate.default_executor.ExecuteExpression(columns[i], update_chunk.data[i]);
		} else {
			D_ASSERT(expressions[i]->type == ExpressionType::BOUND_REF);
			// index into child chunk
			auto &binding = (BoundReferenceExpression &)*expressions[i];
			update_chunk.data[i].Reference(chunk.data[binding.index]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	if (update_is_del_and_insert) {
		// index update or update on complex type, perform a delete and an append instead

		// figure out which rows have not yet been deleted in this update
		// this is required since we might see the same row_id multiple times
		// in the case of an UPDATE query that e.g. has joins
		auto row_id_data = FlatVector::GetData<row_t>(row_ids);
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t update_count = 0;
		for (idx_t i = 0; i < update_chunk.size(); i++) {
			auto row_id = row_id_data[i];
			if (gstate.updated_columns.find(row_id) == gstate.updated_columns.end()) {
				gstate.updated_columns.insert(row_id);
				sel.set_index(update_count++, i);
			}
		}
		if (update_count != update_chunk.size()) {
			// we need to slice here
			update_chunk.Slice(sel, update_count);
		}
		table.Delete(tableref, context.client, row_ids, update_chunk.size());
		// for the append we need to arrange the columns in a specific manner (namely the "standard table order")
		mock_chunk.SetCardinality(update_chunk);
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
		}
		table.Append(tableref, context.client, mock_chunk);
	} else {
		if (return_chunk) {
			mock_chunk.SetCardinality(update_chunk);
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
			}
		}
		table.Update(tableref, context.client, row_ids, columns, update_chunk);
	}

	if (return_chunk) {
		gstate.return_chunk_collection.Append(mock_chunk);
	}

	gstate.updated_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<UpdateGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<UpdateLocalState>(Allocator::Get(context.client), expressions, table.GetTypes(), bound_defaults);
}

void PhysicalUpdate::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (UpdateLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class UpdateSourceState : public GlobalSourceState {
public:
	UpdateSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<UpdateSourceState>();
}

void PhysicalUpdate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (UpdateSourceState &)gstate;
	auto &g = (UpdateGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(g.updated_count));
		state.finished = true;
	}

	idx_t chunk_return = g.returned_chunk_count;
	if (chunk_return >= g.return_chunk_collection.Chunks().size()) {
		return;
	}
	chunk.Reference(g.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((g.return_chunk_collection.GetChunk(chunk_return)).size());
	g.returned_chunk_count += 1;
	if (g.returned_chunk_count >= g.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb
