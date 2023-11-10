#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAttach::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	// check if the database is already attached
	auto &name = info->name;
	const auto &path = info->path;
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = AttachedDatabase::ExtractDatabaseName(path, fs);
	}
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	AccessMode access_mode = config.options.access_mode;
	string type;
	string unrecognized_option;
	for (auto &entry : info->options) {
		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "readwrite" || entry.first == "read_write") {
			auto read_only = !BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "type") {
			type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
		} else if (unrecognized_option.empty()) {
			unrecognized_option = entry.first;
		}
	}

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
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
	auto db_with_same_path = db_manager.GetDatabaseFromPath(context.client, path);
	if (db_with_same_path) {
		throw BinderException("Database \"%s\" is already attached with alias \"%s\"", path,
		                      db_with_same_path->GetName());
	}

	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (type.empty()) {
		// try to extract database type from path
		auto path_and_type = DBPathAndType::Parse(info->path, config);
		type = path_and_type.type;
		info->path = path_and_type.path;
	}

	if (type.empty() && !unrecognized_option.empty()) {
		throw BinderException("Unrecognized option for attach \"%s\"", unrecognized_option);
	}

	// if we are loading a database type from an extension - check if that extension is loaded
	if (!type.empty()) {
		if (!Catalog::TryAutoLoad(context.client, type)) {
			// FIXME: Here it might be preferrable to use an AutoLoadOrThrow kind of function
			// so that either there will be success or a message to throw, and load will be
			// attempted only once respecting the autoloading options
			ExtensionHelper::LoadExternalExtension(context.client, type);
		}
	}

	// attach the database
	auto new_db = db.CreateAttachedDatabase(*info, type, access_mode);
	new_db->Initialize();

	db_manager.AddDatabase(context.client, std::move(new_db));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
