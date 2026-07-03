#include "duckdb/execution/operator/helper/physical_connect.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

SourceResultType PhysicalConnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	// The 4 grammar forms are accepted at parse time. `CONNECT LOCAL` and bare `CONNECT` stay as
	// per-form NotImplementedException placeholders and get swapped in by follow-up PRs.
	if (info->target_is_local) {
		throw NotImplementedException("CONNECT LOCAL is not yet implemented");
	}
	if (info->name.empty()) {
		throw NotImplementedException("CONNECT with no target is not yet implemented");
	}

	auto &client = context.client;

	// At most one active connection; only DISCONNECT clears it (even if the target was detached
	// elsewhere, so the user explicitly acknowledges the broken connection).
	if (client.IsConnected()) {
		auto current = client.TryGetConnectedCatalog();
		throw InvalidInputException("Already connected to \"%s\"; DISCONNECT first before issuing another CONNECT",
		                            current ? current->GetName().GetIdentifierName() : "<detached>");
	}

	auto &db_manager = DatabaseManager::Get(client);
	if (info->name_is_string_literal) {
		// `CONNECT '<uri>'`: attach the connection string under an internal, hidden, ephemeral alias and
		// bind to it in one shot. The name is a random UUID (like __pivot_enum_<uuid>): unique, ASCII
		// (backend-safe), and unguessable, so it is not referenceable in SQL. It is owned by this
		// connection and detached again by DISCONNECT (see PhysicalDisconnect).
		AttachInfo attach_info;
		attach_info.name = Identifier("__connect_" + UUID::ToString(UUID::GenerateRandomUUID()));
		attach_info.path = info->name.GetIdentifierName();
		attach_info.options = info->options;

		auto &config = DBConfig::GetConfig(client);
		AttachOptions options(attach_info.options, config.options.access_mode);
		options.visibility = AttachVisibility::HIDDEN;
		options.ephemeral = true;
		if (options.db_type.empty()) {
			DBPathAndType::ExtractExtensionPrefix(attach_info.path, options.db_type);
		}
		auto target = db_manager.AttachDatabase(client, attach_info, options);
		if (!target->GetCatalog().Supports(RemoteCapability::CONNECT)) {
			// No CONNECT capability: roll back the implicit attach so it does not leak.
			db_manager.DetachDatabase(client, attach_info.name, OnEntryNotFound::RETURN_NULL);
			throw InvalidInputException("CONNECT '%s': the attached database does not support CONNECT",
			                            attach_info.path);
		}
		// Capability confirmed above, so ConnectToCatalog's own support check will not fire.
		client.ConnectToCatalog(target);
		return SourceResultType::FINISHED;
	}

	auto target = db_manager.GetDatabase(info->name);
	if (!target) {
		throw InvalidInputException("Database \"%s\" is not attached", info->name);
	}
	// ConnectToCatalog resolves and caches the dispatch function name; throws if the catalog returns
	// empty from GetConnectFunctionName (i.e. CONNECT is not supported in this context).
	client.ConnectToCatalog(target);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
