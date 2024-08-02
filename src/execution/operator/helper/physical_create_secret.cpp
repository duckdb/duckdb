#include "duckdb/execution/operator/helper/physical_create_secret.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &client = context.client;
	auto &secret_manager = SecretManager::Get(client);

	auto result = secret_manager.CreateSecret(client, info);

	if (result) {
		chunk.SetValue(0, 0, true);
		chunk.SetValue(1, 0, result->secret->ToMapValueShort());
		chunk.SetCardinality(1);
	} else {
		chunk.SetValue(0, 0, false);
		chunk.SetValue(1, 0, Value());
		chunk.SetCardinality(1);
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
