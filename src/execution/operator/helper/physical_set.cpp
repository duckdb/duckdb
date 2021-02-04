#include "duckdb/execution/operator/helper/physical_set.hpp"

namespace duckdb {

void PhysicalSet::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &client = context.client;
	client.set_variables[name] = value; // woop
	state->finished = true;
}

} // namespace duckdb
