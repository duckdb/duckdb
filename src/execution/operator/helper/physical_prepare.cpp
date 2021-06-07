#include "duckdb/execution/operator/helper/physical_prepare.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalPrepare::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                       PhysicalOperatorState *state) const {
	auto &client = context.client;

	// store the prepared statement in the context
	client.prepared_statements[name] = prepared;
	state->finished = true;
}

} // namespace duckdb
