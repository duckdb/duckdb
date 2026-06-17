#include "duckdb/execution/operator/helper/physical_connect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

SourceResultType PhysicalConnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	// The 4 grammar forms are accepted at parse time. This commit implements only
	// `CONNECT <name>` (attached-db identifier); the other forms stay as per-form
	// NotImplementedException placeholders and get swapped in by follow-up PRs.
	if (info->target_is_local) {
		throw NotImplementedException("CONNECT LOCAL is not yet implemented");
	}
	if (info->name.empty()) {
		throw NotImplementedException("CONNECT with no target is not yet implemented");
	}
	if (info->name_is_string_literal) {
		throw NotImplementedException("CONNECT '<connection_string>' is not yet implemented");
	}

	auto &client = context.client;

	// At most one active connection; only DISCONNECT clears it (even if the target was detached
	// elsewhere, so the user explicitly acknowledges the broken connection).
	if (client.IsConnected()) {
		auto current = client.TryGetConnectedCatalog();
		throw InvalidInputException("Already connected to \"%s\"; DISCONNECT first before issuing another CONNECT",
		                            current ? current->GetName().GetIdentifierName() : "<detached>");
	}

	auto target = DatabaseManager::Get(client).GetDatabase(info->name);
	if (!target) {
		throw InvalidInputException("Database \"%s\" is not attached", info->name);
	}
	// ConnectToCatalog resolves and caches the dispatch function name; throws if the catalog returns
	// empty from GetConnectFunctionName (i.e. CONNECT is not supported in this context).
	client.ConnectToCatalog(target);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
