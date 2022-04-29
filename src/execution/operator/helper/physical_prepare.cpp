#include "duckdb/execution/operator/helper/physical_prepare.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

void PhysicalPrepare::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                              LocalSourceState &lstate) const {
	auto &client = context.client;

	// store the prepared statement in the context
	ClientData::Get(client).prepared_statements[name] = prepared;
}

} // namespace duckdb
