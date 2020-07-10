#include "duckdb/execution/operator/scan/physical_empty_result.hpp"

using namespace std;

namespace duckdb {

void PhysicalEmptyResult::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	state->finished = true;
}

} // namespace duckdb
