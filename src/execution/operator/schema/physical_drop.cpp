#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalDrop::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT: {
		// DEALLOCATE silently ignores errors
		auto &statements = ClientData::Get(context.client).prepared_statements;
		auto stmt_iter = statements.find(info->name);
		if (stmt_iter != statements.end()) {
			statements.erase(stmt_iter);
		}
		break;
	}
	case CatalogType::SCHEMA_ENTRY: {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.DropEntry(context.client, *info);
		// PG-compatible: leave search_path alone. The dropped schema becomes an
		// invalid entry that lookups will simply skip (see GetResolvedDefault).
		break;
	}
	case CatalogType::SECRET_ENTRY: {
		// Note: the schema param is used to optionally pass the storage to drop from
		D_ASSERT(info->extra_drop_info);
		auto &extra_info = info->extra_drop_info->Cast<ExtraDropSecretInfo>();
		SecretManager::Get(context.client)
		    .DropSecretByName(context.client, info->name, info->if_not_found, extra_info.persist_mode,
		                      extra_info.secret_storage);
		break;
	}
	default: {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.DropEntry(context.client, *info);
		break;
	}
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
