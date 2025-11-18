#include "duckdb/execution/operator/helper/physical_create_secret.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &client = context.client;
	auto &secret_manager = SecretManager::Get(client);

	secret_manager.CreateSecret(client, create_input);

	chunk.SetValue(0, 0, true);
	chunk.SetCardinality(1);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
