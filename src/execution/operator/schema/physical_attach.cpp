#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/database_path_and_type.hpp"

namespace duckdb {

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
	if (options.db_type.empty()) {
		DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
	}
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = AttachedDatabase::ExtractDatabaseName(path, fs);
	}

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ||
	    info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// constant-time lookup in the catalog for the db name
		auto existing_db = db_manager.GetDatabase(name);
		if (existing_db) {
			if ((existing_db->IsReadOnly() && options.access_mode == AccessMode::READ_WRITE) ||
			    (!existing_db->IsReadOnly() && options.access_mode == AccessMode::READ_ONLY)) {
				auto existing_mode = existing_db->IsReadOnly() ? AccessMode::READ_ONLY : AccessMode::READ_WRITE;
				auto existing_mode_str = EnumUtil::ToString(existing_mode);
				auto attached_mode = EnumUtil::ToString(options.access_mode);
				throw BinderException("Database \"%s\" is already attached in %s mode, cannot re-attach in %s mode",
				                      name, existing_mode_str, attached_mode);
			}
			if (!options.default_table.name.empty()) {
				existing_db->GetCatalog().SetDefaultTable(options.default_table.schema, options.default_table.name);
			}
			if (info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
				// allow custom catalogs to override this behavior
				if (!existing_db->GetCatalog().HasConflictingAttachOptions(path, options)) {
					return SourceResultType::FINISHED;
				}
			} else {
				return SourceResultType::FINISHED;
			}
		}
	}

	db_manager.AttachDatabase(context.client, *info, options);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
