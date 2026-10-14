#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/database_path_and_type.hpp"

namespace duckdb {

//! Warn when a DuckLake metadata catalog is attached as a plain DuckDB database
static void WarnIfDuckLakeMetadata(ClientContext &context, AttachedDatabase &attached_db, const string &path) {
	auto &catalog = attached_db.GetCatalog();
	if (!catalog.IsDuckCatalog()) {
		return;
	}
	auto default_schema = catalog.GetDefaultSchema();
	if (!default_schema) {
		return;
	}
	// scan the committed entries, a catalog lookup would open a transaction on the database
	bool has_metadata_table = false;
	catalog.Cast<DuckCatalog>().ScanSchemas(VisibilityBound::AllCommitted(), [&](DuckSchemaEntry &schema) {
		if (schema.name != *default_schema) {
			return;
		}
		schema.Scan(CatalogType::TABLE_ENTRY, VisibilityBound::AllCommitted(), [&](CatalogEntry &entry) {
			if (entry.name == "ducklake_metadata") {
				has_metadata_table = true;
			}
		});
	});
	if (!has_metadata_table) {
		return;
	}
	DUCKDB_LOG_WARNING(context,
	                   "\"%s\" is a DuckLake metadata catalog, attached as a DuckDB database: only the DuckLake "
	                   "metadata tables are visible. Use \"ATTACH 'ducklake:%s'\" to attach the DuckLake, or "
	                   "\"(TYPE duckdb)\" to attach the metadata catalog without this warning.",
	                   path, path);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAttach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	// construct the options
	AttachOptions options(info->options, config.options.access_mode);

	// get the name and path of the database
	auto &name = info->name;
	auto &path = info->path;
	// preserve the verbatim path before extension-prefix stripping
	options.original_path = path;
	if (options.db_type.empty()) {
		DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
	}
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = Identifier(AttachedDatabase::ExtractDatabaseName(path, fs));
	}

	// an explicit TYPE duckdb or a hidden attach (e.g. by DuckLake itself) opens the metadata on purpose
	bool warn_ducklake_metadata = options.db_type.empty() && options.visibility == AttachVisibility::SHOWN;

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	auto attached_db = db_manager.AttachDatabase(context.client, *info, options);
	if (warn_ducklake_metadata) {
		WarnIfDuckLakeMetadata(context.client, *attached_db, path);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
