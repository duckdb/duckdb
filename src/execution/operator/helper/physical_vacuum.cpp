#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

using namespace std;

namespace duckdb {

void PhysicalVacuum::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	// NOP
	state->finished = true;
}

} // namespace duckdb
