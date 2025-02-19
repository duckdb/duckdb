#include "duckdb/main/database_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db) : next_oid(0), current_query_number(1) {
	system = make_uniq<AttachedDatabase>(db);
	databases = make_uniq<CatalogSet>(system->GetCatalog());
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	// The SYSTEM_DATABASE has no persistent storage.
	system->Initialize();
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabase(ClientContext &context, const string &name) {
	if (StringUtil::Lower(name) == TEMP_CATALOG) {
		return context.client_data->temporary_objects.get();
	}
	if (StringUtil::Lower(name) == SYSTEM_CATALOG) {
		return system;
	}
	return reinterpret_cast<AttachedDatabase *>(databases->GetEntry(context, name).get());
}

optional_ptr<AttachedDatabase> DatabaseManager::AttachDatabase(ClientContext &context, const AttachInfo &info,
                                                               const AttachOptions &options) {
	if (AttachedDatabase::NameIsReserved(info.name)) {
		throw BinderException("Attached database name \"%s\" cannot be used because it is a reserved name", info.name);
	}
	// now create the attached database
	auto &db = DatabaseInstance::GetDatabase(context);
	auto attached_db = db.CreateAttachedDatabase(context, info, options);

	if (options.db_type.empty()) {
		InsertDatabasePath(context, info.path, attached_db->name);
	}

	const auto name = attached_db->GetName();
	attached_db->oid = NextOid();
	LogicalDependencyList dependencies;
	if (default_database.empty()) {
		default_database = name;
	}

	// and add it to the databases catalog set
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DetachDatabase(context, name, OnEntryNotFound::RETURN_NULL);
	}
	if (!databases->CreateEntry(context, name, std::move(attached_db), dependencies)) {
		throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
	}

	return GetDatabase(context, name);
}

void DatabaseManager::DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found) {
	if (GetDefaultDatabase(context) == name) {
		throw BinderException("Cannot detach database \"%s\" because it is the default database. Select a different "
		                      "database using `USE` to allow detaching this database",
		                      name);
	}

	if (!databases->DropEntry(context, name, false, true)) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Failed to detach database with name \"%s\": database not found", name);
		}
	}
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabaseFromPath(ClientContext &context, const string &path) {
	auto database_list = GetDatabases(context);
	for (auto &db_ref : database_list) {
		auto &db = db_ref.get();
		if (db.IsSystem()) {
			continue;
		}
		auto &catalog = Catalog::GetCatalog(db);
		if (catalog.InMemory()) {
			continue;
		}
		auto db_path = catalog.GetDBPath();
		if (StringUtil::CIEquals(path, db_path)) {
			return &db;
		}
	}
	return nullptr;
}

void DatabaseManager::CheckPathConflict(ClientContext &context, const string &path) {
	// ensure that we did not already attach a database with the same path
	bool path_exists;
	{
		lock_guard<mutex> path_lock(db_paths_lock);
		path_exists = db_paths.find(path) != db_paths.end();
	}
	if (path_exists) {
		// check that the database is actually still attached
		auto entry = GetDatabaseFromPath(context, path);
		if (entry) {
			throw BinderException("Unique file handle conflict: Database \"%s\" is already attached with path \"%s\", ",
			                      entry->name, path);
		}
	}
}

void DatabaseManager::InsertDatabasePath(ClientContext &context, const string &path, const string &name) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}

	CheckPathConflict(context, path);
	lock_guard<mutex> path_lock(db_paths_lock);
	db_paths.insert(path);
}

void DatabaseManager::EraseDatabasePath(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto path_it = db_paths.find(path);
	if (path_it != db_paths.end()) {
		db_paths.erase(path_it);
	}
}

vector<string> DatabaseManager::GetAttachedDatabasePaths() {
	lock_guard<mutex> path_lock(db_paths_lock);
	vector<string> paths;
	for (auto &path : db_paths) {
		paths.push_back(path);
	}
	return paths;
}

void DatabaseManager::GetDatabaseType(ClientContext &context, AttachInfo &info, const DBConfig &config,
                                      AttachOptions &options) {

	// Test if the database is a DuckDB database file.
	if (StringUtil::CIEquals(options.db_type, "DUCKDB")) {
		options.db_type = "";
		return;
	}

	// Try to extract the database type from the path.
	if (options.db_type.empty()) {
		CheckPathConflict(context, info.path);

		auto &fs = FileSystem::GetFileSystem(context);
		DBPathAndType::CheckMagicBytes(fs, info.path, options.db_type);
	}

	// If we are loading a database type from an extension, then we need to check if that extension is loaded.
	if (!options.db_type.empty()) {
		if (!Catalog::TryAutoLoad(context, options.db_type)) {
			// FIXME: Here it might be preferable to use an AutoLoadOrThrow kind of function
			// so that either there will be success or a message to throw, and load will be
			// attempted only once respecting the auto-loading options
			ExtensionHelper::LoadExternalExtension(context, options.db_type);
		}
		return;
	}
}

const string &DatabaseManager::GetDefaultDatabase(ClientContext &context) {
	auto &config = ClientData::Get(context);
	auto &default_entry = config.catalog_search_path->GetDefault();
	if (IsInvalidCatalog(default_entry.catalog)) {
		auto &result = DatabaseManager::Get(context).default_database;
		if (result.empty()) {
			throw InternalException("Calling DatabaseManager::GetDefaultDatabase with no default database set");
		}
		return result;
	}
	return default_entry.catalog;
}

// LCOV_EXCL_START
void DatabaseManager::SetDefaultDatabase(ClientContext &context, const string &new_value) {
	auto db_entry = GetDatabase(context, new_value);

	if (!db_entry) {
		throw InternalException("Database \"%s\" not found", new_value);
	} else if (db_entry->IsTemporary()) {
		throw InternalException("Cannot set the default database to a temporary database");
	} else if (db_entry->IsSystem()) {
		throw InternalException("Cannot set the default database to a system database");
	}

	default_database = new_value;
}
// LCOV_EXCL_STOP

vector<reference<AttachedDatabase>> DatabaseManager::GetDatabases(ClientContext &context) {
	vector<reference<AttachedDatabase>> result;
	databases->Scan(context, [&](CatalogEntry &entry) { result.push_back(entry.Cast<AttachedDatabase>()); });
	result.push_back(*system);
	result.push_back(*context.client_data->temporary_objects);
	return result;
}

void DatabaseManager::ResetDatabases(unique_ptr<TaskScheduler> &scheduler) {
	vector<reference<AttachedDatabase>> result;
	databases->Scan([&](CatalogEntry &entry) { result.push_back(entry.Cast<AttachedDatabase>()); });
	for (auto &database : result) {
		database.get().Close();
	}
	scheduler.reset();
	databases.reset();
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
