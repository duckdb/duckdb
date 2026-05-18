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

	// CONNECT <name> — actual implementation.
	auto &client = context.client;

	// At most one active binding at a time; the only way off a bound state is DISCONNECT.
	// Sidesteps cross-backend transaction-state ambiguity; multi-binding can be relaxed later
	// without breaking existing scripts.
	if (client.IsBoundToCatalog()) {
		auto current = client.TryGetBoundCatalog();
		throw InvalidInputException("Already connected to \"%s\"; DISCONNECT first before issuing another CONNECT",
		                            current ? current->GetName() : "<detached>");
	}

	auto target = DatabaseManager::Get(client).GetDatabase(info->name);
	if (!target) {
		throw InvalidInputException("Database \"%s\" is not attached", info->name);
	}
	auto ext = target->GetStorageExtension();
	if (!ext || !ext->SupportsPassthrough()) {
		throw InvalidInputException(
		    "Database \"%s\" does not support pass-through SQL execution (CONNECT). The storage extension must "
		    "override StorageExtension::SupportsPassthrough() and the catalog must implement "
		    "Catalog::GetConnectFunction().",
		    info->name);
	}
	client.BindToCatalog(target);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
