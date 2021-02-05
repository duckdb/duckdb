#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void PhysicalSet::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &db = context.client.db;
	db->config.set_variables[name] = value; // woop
	state->finished = true;
}

} // namespace duckdb
