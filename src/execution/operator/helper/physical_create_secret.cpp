#include "duckdb/execution/operator/helper/physical_create_secret.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret_manager.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &client = context.client;

	// TODO: we should probably not have to do the lookup again here: either not pass it through at all or use the fun
	// from input. We could also not pass through the function during bind at all, just check its existence.
	context.client.db->config.secret_manager->CreateSecret(client, info);

	chunk.SetValue(0, 0, true);
	chunk.SetCardinality(1);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
