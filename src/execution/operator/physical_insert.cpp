#include "execution/operator/physical_insert.hpp"
#include "execution/expression_executor.hpp"

#include "common/types/chunk_collection.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include "main/client_context.hpp"

#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalInsert::GetNames() {
	return {"Count"};
}
vector<TypeId> PhysicalInsert::GetTypes() {
	return {TypeId::BIGINT};
}

void PhysicalInsert::_GetChunk(ClientContext &context, DataChunk &chunk,
                               PhysicalOperatorState *state) {

	chunk.Reset();

	if (state->finished) {
		return;
	}

	int64_t insert_count = 0;
	if (children.size() > 0) {
		// insert from SELECT statement

		// the child can include a scan of the table we are inserting from
		// hence if we directly append to the same table, we will get into an
		// infinite loop instead, buffer all the entries
		ChunkCollection collection;
		while (true) {
			children[0]->GetChunk(context, state->child_chunk,
			                      state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
			collection.Append(state->child_chunk);
		}

		// create the chunk to insert from
		DataChunk insert_chunk;
		auto types = table->GetTypes();

		insert_chunk.Initialize(types);
		for (auto &chunkptr : collection.chunks) {
			auto &chunk = *chunkptr;
			if (column_index_map.size() > 0) {
				// columns specified by the user, use column_index_map
				for (size_t i = 0; i < table->columns.size(); i++) {
					if (column_index_map[i] < 0) {
						// insert default value
						insert_chunk.data[i].count = chunk.size();
						VectorOperations::Set(insert_chunk.data[i],
						                      table->columns[i].default_value);
					} else {
						// get value from child chunk
						assert((size_t)column_index_map[i] <
						       chunk.column_count);
						if (insert_chunk.data[i].type ==
						    chunk.data[column_index_map[i]].type) {
							// matching type, reference
							insert_chunk.data[i].Reference(
							    chunk.data[column_index_map[i]]);
						} else {
							// non-matching type, cast
							VectorOperations::Cast(
							    chunk.data[column_index_map[i]],
							    insert_chunk.data[i]);
						}
					}
				}
			} else {
				// no columns specified, just append directly
				for (size_t i = 0; i < insert_chunk.column_count; i++) {
					if (insert_chunk.data[i].type == chunk.data[i].type) {
						// matching type, reference
						insert_chunk.data[i].Reference(chunk.data[i]);
					} else {
						// non-matching type, cast
						VectorOperations::Cast(chunk.data[i],
						                       insert_chunk.data[i]);
					}
				}
			}
			table->storage->Append(context, insert_chunk);
			insert_count += chunk.size();
		}
	} else {
		// insert from constant values
		// create the chunks to insert from
		DataChunk insert_chunk, temp_chunk;
		auto types = table->GetTypes();

		insert_chunk.Initialize(types);
		temp_chunk.Initialize(types);
		ExpressionExecutor executor(children.size() == 0 ? nullptr : state,
		                            context);

		// loop over all the constants
		for (auto &list : insert_values) {
			if (column_index_map.size() > 0) {
				// columns specified by the user, use column_index_map
				for (size_t i = 0; i < table->columns.size(); i++) {
					if (column_index_map[i] < 0) {
						// insert default value
						size_t index = insert_chunk.data[i].count++;
						insert_chunk.data[i].SetValue(
						    index, table->columns[i].default_value);
					} else {
						// get value from constants
						assert(column_index_map[i] < (int)list.size());
						auto &expr = list[column_index_map[i]];
						executor.ExecuteExpression(expr.get(),
						                           temp_chunk.data[i]);
						assert(temp_chunk.data[i].count == 1);
						// append to the insert chunk
						insert_chunk.data[i].Append(temp_chunk.data[i]);
					}
				}
			} else {
				// no columns specified
				for (size_t i = 0; i < list.size(); i++) {
					// execute the expressions to get the values
					auto &expr = list[i];
					if (expr->type == ExpressionType::VALUE_DEFAULT) {
						temp_chunk.data[i].count = 1;
						temp_chunk.data[i].SetValue(
						    0, table->columns[i].default_value);
					} else {
						executor.ExecuteExpression(expr.get(),
						                           temp_chunk.data[i]);
					}
					assert(temp_chunk.data[i].count == 1);
					// append to the insert chunk
					insert_chunk.data[i].Append(temp_chunk.data[i]);
				}
			}
			if (insert_chunk.size() == STANDARD_VECTOR_SIZE) {
				// flush the chunk if it is full
				table->storage->Append(context, insert_chunk);
				insert_count += insert_chunk.size();
				insert_chunk.Reset();
			}
		}
		if (insert_chunk.size() > 0) {
			// append any remaining elements to the table
			table->storage->Append(context, insert_chunk);
			insert_count += insert_chunk.size();
		}
	}

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(insert_count));

	state->finished = true;
}

unique_ptr<PhysicalOperatorState>
PhysicalInsert::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(
	    children.size() == 0 ? nullptr : children[0].get(), parent_executor);
}
