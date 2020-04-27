#include "duckdb/execution/operator/helper/physical_vacuum.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalVacuum::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	// NOP
	state->finished = true;
}
