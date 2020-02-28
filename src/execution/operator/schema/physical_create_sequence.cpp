#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateSequence::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	context.catalog.CreateSequence(context, info.get());
	state->finished = true;
}
