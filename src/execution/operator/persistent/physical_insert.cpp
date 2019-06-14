#include "execution/operator/persistent/physical_insert.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalInsert::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t insert_count = 0;
	if (children.size() > 0) {
		// insert from SELECT statement

		// create the chunk to insert from
		DataChunk insert_chunk;
		auto types = table->GetTypes();

		insert_chunk.Initialize(types);
		while (true) {
			// scan the children for chunks to insert
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
			auto &chunk = state->child_chunk;

			state->child_chunk.Flatten();

			ExpressionExecutor executor(chunk);
			insert_chunk.Reset();
			if (column_index_map.size() > 0) {
				// columns specified by the user, use column_index_map
				for (index_t i = 0; i < table->columns.size(); i++) {
					if (column_index_map[i] == INVALID_INDEX) {
						// insert default value
						executor.ExecuteExpression(*bound_defaults[i], insert_chunk.data[i]);
					} else {
						// get value from child chunk
						assert((index_t)column_index_map[i] < chunk.column_count);
						assert(insert_chunk.data[i].type == chunk.data[column_index_map[i]].type);
						insert_chunk.data[i].Reference(chunk.data[column_index_map[i]]);
					}
				}
			} else {
				// no columns specified, just append directly
				for (index_t i = 0; i < insert_chunk.column_count; i++) {
					assert(insert_chunk.data[i].type == chunk.data[i].type);
					insert_chunk.data[i].Reference(chunk.data[i]);
				}
			}
			table->storage->Append(*table, context, insert_chunk);
			insert_count += chunk.size();
		}
	} else {
		// insert from constant values
		// create the chunks to insert from
		DataChunk insert_chunk, temp_chunk;
		auto types = table->GetTypes();

		insert_chunk.Initialize(types);
		temp_chunk.Initialize(types);
		ExpressionExecutor executor;

		// loop over all the constants
		for (auto &list : insert_values) {
			if (column_index_map.size() > 0) {
				// columns specified by the user, use column_index_map
				for (index_t i = 0; i < table->columns.size(); i++) {
					if (column_index_map[i] == INVALID_INDEX) {
						// insert default value
						executor.ExecuteExpression(*bound_defaults[i], temp_chunk.data[i]);
					} else {
						// get value from constants
						assert(column_index_map[i] < list.size());
						auto &expr = list[column_index_map[i]];
						executor.ExecuteExpression(*expr, temp_chunk.data[i]);
					}
					assert(temp_chunk.data[i].count == 1);
					// append to the insert chunk
					insert_chunk.data[i].Append(temp_chunk.data[i]);
				}
			} else {
				// no columns specified
				for (index_t i = 0; i < list.size(); i++) {
					// execute the expressions to get the values
					auto &expr = list[i];
					if (expr->type == ExpressionType::VALUE_DEFAULT) {
						executor.ExecuteExpression(*bound_defaults[i], temp_chunk.data[i]);
					} else {
						executor.ExecuteExpression(*expr, temp_chunk.data[i]);
					}
					assert(temp_chunk.data[i].count == 1);
					// append to the insert chunk
					insert_chunk.data[i].Append(temp_chunk.data[i]);
				}
			}
			if (insert_chunk.size() == STANDARD_VECTOR_SIZE) {
				// flush the chunk if it is full
				table->storage->Append(*table, context, insert_chunk);
				insert_count += insert_chunk.size();
				insert_chunk.Reset();
			}
		}
		if (insert_chunk.size() > 0) {
			// append any remaining elements to the table
			table->storage->Append(*table, context, insert_chunk);
			insert_count += insert_chunk.size();
		}
	}

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(insert_count));

	state->finished = true;
}
