#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const {
	D_ASSERT(scope == SetScope::GLOBAL || scope == SetScope::SESSION);

	if (scope == SetScope::GLOBAL) {
		context.client.db->config.set_variables[name] = value;
	} else {
		context.client.set_variables[name] = value;
	}

	state->finished = true;
}

} // namespace duckdb
