#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class UpdateGlobalState : public GlobalOperatorState {
public:
	UpdateGlobalState() : updated_count(0) {
	}

	mutex lock;
	idx_t updated_count;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(vector<unique_ptr<Expression>> &expressions, vector<LogicalType> &table_types,
	                 vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(bound_defaults) {
		// initialize the update chunk
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->return_type);
		}
		update_chunk.Initialize(update_types);
		// initialize the mock chunk
		mock_chunk.Initialize(table_types);
	}

	DataChunk update_chunk;
	DataChunk mock_chunk;
	ExpressionExecutor default_executor;
};

void PhysicalUpdate::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                          DataChunk &chunk) const {
	auto &gstate = (UpdateGlobalState &)state;
	auto &ustate = (UpdateLocalState &)lstate;

	DataChunk &update_chunk = ustate.update_chunk;
	DataChunk &mock_chunk = ustate.mock_chunk;

	chunk.Normalify();
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
	if (is_index_update) {
		// index update, perform a delete and an append instead
		table.Delete(tableref, context.client, row_ids, update_chunk.size());
		mock_chunk.SetCardinality(update_chunk);
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
		}
		table.Append(tableref, context.client, mock_chunk);
	} else {
		table.Update(tableref, context.client, row_ids, columns, update_chunk);
	}
	gstate.updated_count += chunk.size();
}

unique_ptr<GlobalOperatorState> PhysicalUpdate::GetGlobalState(ClientContext &context) {
	return make_unique<UpdateGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalUpdate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<UpdateLocalState>(expressions, table.types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalUpdate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const {
	auto &gstate = (UpdateGlobalState &)*sink_state;

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(gstate.updated_count));

	state->finished = true;
}
void PhysicalUpdate::Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	auto &state = (UpdateLocalState &)lstate;
	context.thread.profiler.Flush(this, &state.default_executor, "default_executor", 1);
	context.client.profiler->Flush(context.thread.profiler);
}

} // namespace duckdb
