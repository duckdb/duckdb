#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

namespace duckdb {

void PhysicalVacuum::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	// NOP
	state->finished = true;
}

} // namespace duckdb
