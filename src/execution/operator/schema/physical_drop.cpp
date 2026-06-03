#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/sql_identifier.hpp"

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

		// Check if the dropped schema was set as the current schema
		auto &client_data = ClientData::Get(context.client);
		auto &default_entry = client_data.catalog_search_path->GetDefault();
		auto &current_catalog = default_entry.catalog;
		auto &current_schema = default_entry.schema;
		D_ASSERT(info->name != DEFAULT_SCHEMA);

		if (info->catalog == current_catalog && current_schema == info->name) {
			// Reset the schema to default
			SchemaSetting::SetLocal(context.client, DEFAULT_SCHEMA);
		}
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
	case CatalogType::TRIGGER_ENTRY: {
		// Triggers are stored per-table; must be dropped through the table, not the schema
		if (!info->extra_drop_info) {
			throw InternalException("DROP TRIGGER: missing extra_drop_info (expected ExtraDropTriggerInfo)");
		}
		auto &trigger_extra = info->extra_drop_info->Cast<ExtraDropTriggerInfo>();
		if (!trigger_extra.base_table) {
			throw InternalException("DROP TRIGGER: ExtraDropTriggerInfo has no base_table");
		}
		auto &base_table_ref = trigger_extra.base_table->Cast<BaseTableRef>();
		auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context.client, info->catalog, info->schema,
		                                                         base_table_ref.table_name);
		auto &duck_table = table_entry.Cast<DuckTableEntry>();
		auto transaction = duck_table.catalog.GetCatalogTransaction(context.client);
		if (!duck_table.DropTrigger(transaction, info->name, info->cascade)) {
			if (info->if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
				throw CatalogException("Trigger with name \"%s\" does not exist on table \"%s\"", info->name,
				                       base_table_ref.table_name);
			}
		}
		break;
	}
	case CatalogType::FEATURE_ENTRY: {
		// Look up the feature to get version info before dropping
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		auto &schema = catalog.GetSchema(context.client, info->schema);
		auto transaction = catalog.GetCatalogTransaction(context.client);
		auto entry = schema.GetEntry(transaction, CatalogType::FEATURE_ENTRY, info->name);

		int64_t current_version = 0;
		int64_t retain_versions = 1;
		if (entry) {
			auto &feat = entry->Cast<FeatureCatalogEntry>();
			current_version = feat.current_version;
			retain_versions = feat.retain_versions;
		}

		// Drop the feature catalog entry; ownership cascades to the view
		catalog.DropEntry(context.client, *info);

		// Clean up version tables
		if (current_version > 0) {
			auto &db = DatabaseInstance::GetDatabase(context.client);
			Connection con(db);
			if (!info->catalog.empty()) {
				con.Query("USE " + SQLIdentifier::ToString(info->catalog));
			}
			if (!info->schema.empty() && info->schema != DEFAULT_SCHEMA) {
				con.Query("SET schema = '" + info->schema + "'");
			}
			int64_t min_version = current_version - retain_versions + 1;
			if (min_version < 1) {
				min_version = 1;
			}
			for (int64_t v = min_version; v <= current_version; v++) {
				auto table_name = info->name + "__v" + duckdb::to_string(v);
				con.Query("DROP TABLE IF EXISTS " + SQLIdentifier::ToString(table_name));
			}
		}
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
