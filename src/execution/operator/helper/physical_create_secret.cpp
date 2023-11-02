#include "duckdb/execution/operator/helper/physical_create_secret.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret_manager.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &client = context.client;

	// TODO: we should probably check availability before calling into a registered create secret?

	// Call create secret function
	CreateSecretInput secret_input {
	    info.type,
	    info.provider,
	    info.name,
	    info.scope,
	    info.named_parameters
	};
	auto secret = function.function(client, secret_input);

	if (!secret) {
		throw InternalException("CreateSecretFunction '" + function.name + "' did not return a secret!");
	}

	// Register the secret at the secret_manager
	context.client.db->config.secret_manager->RegisterSecret(secret, info.on_conflict);

	// TODO return stuff?

	chunk.SetValue(0,0,true);
	chunk.SetCardinality(1);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
