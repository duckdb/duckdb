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
// Helper
//===--------------------------------------------------------------------===//

void ParseOptions(const unique_ptr<AttachInfo> &info, AccessMode &access_mode, string &db_type,
                  string &unrecognized_option) {

	for (auto &entry : info->options) {

		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "readwrite" || entry.first == "read_write") {
			auto read_only = !BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "type") {
			// extract the database type
			db_type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
			continue;
		}

		// we allow unrecognized options
		if (unrecognized_option.empty()) {
			unrecognized_option = entry.first;
		}
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAttach::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	AccessMode access_mode = config.options.access_mode;
	string db_type;
	string unrecognized_option;
	ParseOptions(info, access_mode, db_type, unrecognized_option);

	// get the name and path of the database
	auto &name = info->name;
	auto &path = info->path;
	if (db_type.empty()) {
		DBPathAndType::ExtractExtensionPrefix(path, db_type);
	}
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = AttachedDatabase::ExtractDatabaseName(path, fs);
	}

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		// constant-time lookup in the catalog for the db name
		auto existing_db = db_manager.GetDatabase(context.client, name);
		if (existing_db) {

			if ((existing_db->IsReadOnly() && access_mode == AccessMode::READ_WRITE) ||
			    (!existing_db->IsReadOnly() && access_mode == AccessMode::READ_ONLY)) {

				auto existing_mode = existing_db->IsReadOnly() ? AccessMode::READ_ONLY : AccessMode::READ_WRITE;
				auto existing_mode_str = EnumUtil::ToString(existing_mode);
				auto attached_mode = EnumUtil::ToString(access_mode);
				throw BinderException("Database \"%s\" is already attached in %s mode, cannot re-attach in %s mode",
				                      name, existing_mode_str, attached_mode);
			}

			return SourceResultType::FINISHED;
		}
	}

	// get the database type and attach the database
	db_manager.GetDatabaseType(context.client, db_type, *info, config, unrecognized_option);
	auto attached_db = db_manager.AttachDatabase(context.client, *info, db_type, access_mode);
	attached_db->Initialize();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
