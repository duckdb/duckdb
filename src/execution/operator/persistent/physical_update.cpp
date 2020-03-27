#include "duckdb/execution/operator/persistent/physical_update.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalUpdate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	vector<TypeId> update_types;
	for (auto &expr : expressions) {
		update_types.push_back(expr->return_type);
	}
	DataChunk update_chunk;
	update_chunk.Initialize(update_types);

	// initialize states for bound default expressions
	ExpressionExecutor default_executor(bound_defaults);

	int64_t updated_count = 0;

	DataChunk mock_chunk;
	if (is_index_update) {
		mock_chunk.Initialize(table.types);
	}

	while (true) {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		state->child_chunk.Normalify();
		default_executor.SetChunk(state->child_chunk);

		// update data in the base table
		// the row ids are given to us as the last column of the child chunk
		auto &row_ids = state->child_chunk.data[state->child_chunk.column_count() - 1];
		update_chunk.SetCardinality(state->child_chunk);
		for (idx_t i = 0; i < expressions.size(); i++) {
			if (expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
				// default expression, set to the default value of the column
				default_executor.ExecuteExpression(columns[i], update_chunk.data[i]);
			} else {
				assert(expressions[i]->type == ExpressionType::BOUND_REF);
				// index into child chunk
				auto &binding = (BoundReferenceExpression &)*expressions[i];
				update_chunk.data[i].Reference(state->child_chunk.data[binding.index]);
			}
		}

		if (is_index_update) {
			// index update, perform a delete and an append instead
			table.Delete(tableref, context, row_ids, update_chunk.size());
			mock_chunk.SetCardinality(update_chunk);
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
			}
			table.Append(tableref, context, mock_chunk);
		} else {
			table.Update(tableref, context, row_ids, columns, update_chunk);
		}
		updated_count += state->child_chunk.size();
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(updated_count));

	state->finished = true;

	chunk.Verify();
}
