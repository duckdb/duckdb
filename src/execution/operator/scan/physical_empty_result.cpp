#include "duckdb/execution/operator/scan/physical_empty_result.hpp"

namespace duckdb {

void PhysicalEmptyResult::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                           PhysicalOperatorState *state) const {
	state->finished = true;
}

} // namespace duckdb
