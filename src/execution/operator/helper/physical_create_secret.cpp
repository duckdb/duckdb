#include "duckdb/execution/operator/helper/physical_create_secret.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &client = context.client;

	// TODO: we should probably check credentials before calling into a registered create secret?

	// Call create secret function
	CreateSecretInput secret_input {
	    info.type,
	    info.provider,
	    info.name,
	    info.scope,
	    info.named_parameters
	};
	auto credential = function.function(client, secret_input);

	if (!credential) {
		throw InternalException("Create secret function '" + function.name + "' did not return credentials!");
	}

	// Register the credentials at the credential manager
	context.client.db->config.credential_manager->RegisterCredentials(credential, info.on_conflict);

	// TODO return stuff?

	chunk.SetValue(0,0,true);
	chunk.SetCardinality(1);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
