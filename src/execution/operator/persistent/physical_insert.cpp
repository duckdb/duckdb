#include "duckdb/execution/operator/persistent/physical_insert.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalInsert::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {

	int64_t insert_count = 0;
	// create the chunk to insert from
	DataChunk insert_chunk;
	auto types = table->GetTypes();

	// initialize executor for bound default expressions
	ExpressionExecutor default_executor(bound_defaults);

	insert_chunk.Initialize(types);
	while (true) {
		// scan the children for chunks to insert
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		auto &chunk = state->child_chunk;

		chunk.Normalify();
		default_executor.SetChunk(chunk);

		insert_chunk.Reset();
		insert_chunk.SetCardinality(chunk);
		if (column_index_map.size() > 0) {
			// columns specified by the user, use column_index_map
			for (idx_t i = 0; i < table->columns.size(); i++) {
				if (column_index_map[i] == INVALID_INDEX) {
					// insert default value
					default_executor.ExecuteExpression(i, insert_chunk.data[i]);
				} else {
					// get value from child chunk
					assert((idx_t)column_index_map[i] < chunk.column_count());
					assert(insert_chunk.data[i].type == chunk.data[column_index_map[i]].type);
					insert_chunk.data[i].Reference(chunk.data[column_index_map[i]]);
				}
			}
		} else {
			// no columns specified, just append directly
			for (idx_t i = 0; i < insert_chunk.column_count(); i++) {
				assert(insert_chunk.data[i].type == chunk.data[i].type);
				insert_chunk.data[i].Reference(chunk.data[i]);
			}
		}
		table->storage->Append(*table, context, insert_chunk);
		insert_count += chunk.size();
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_count));

	state->finished = true;
}
