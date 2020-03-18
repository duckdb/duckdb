#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDelete::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t deleted_count = 0;
	while (true) {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		// delete data in the base table
		// the row ids are given to us as the last column of the child chunk
		table.Delete(tableref, context, state->child_chunk.data[row_id_index], state->child_chunk.size());
		deleted_count += state->child_chunk.size();
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(deleted_count));

	state->finished = true;
}
