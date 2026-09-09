#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &client = context.client;
	// IsConnected() so DISCONNECT still works when the target was detached elsewhere.
	if (!client.IsConnected()) {
		throw InvalidInputException("DISCONNECT: no active CONNECT (already on LOCAL)");
	}
	// Capture the target before clearing the binding so we can reap an ephemeral attachment.
	auto target = client.TryGetConnectedCatalog();
	client.DisconnectFromCatalog();
	if (target && target->IsEphemeral()) {
		// `CONNECT '<uri>'` attached this hidden catalog implicitly; detach it now.
		DatabaseManager::Get(client).DetachDatabase(client, target->GetName(), OnEntryNotFound::RETURN_NULL);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
