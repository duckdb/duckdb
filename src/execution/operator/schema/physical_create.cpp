
#include "execution/operator/schema/physical_create.hpp"
#include "execution/expression_executor.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"

#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreate::_GetChunk(ClientContext &context, DataChunk &chunk,
                               PhysicalOperatorState *state) {
	int64_t inserted_count = 0;
	if (children.size() > 0) {
		// children[0]->GetChunk(context, state->child_chunk,
		//                       state->child_state.get());
		// if (state->child_chunk.count == 0) {
		// 	break;
		// }
		throw NotImplementedException(
		    "CREATE TABLE from SELECT not supported yet");
	} else {
		schema->CreateTable(context.ActiveTransaction(), info.get());
	}

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(inserted_count));

	state->finished = true;
}
