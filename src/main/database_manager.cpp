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

DatabaseManager::DatabaseManager(DatabaseInstance &db)
    : next_oid(0), current_query_number(1), current_transaction_id(0) {
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

void DatabaseManager::FinalizeStartup() {
	auto dbs = GetDatabases();
	for (auto &db : dbs) {
		db.get().FinalizeLoad(nullptr);
	}
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

optional_ptr<AttachedDatabase> DatabaseManager::AttachDatabase(ClientContext &context, AttachInfo &info,
                                                               AttachOptions &options) {
	if (AttachedDatabase::NameIsReserved(info.name)) {
		throw BinderException("Attached database name \"%s\" cannot be used because it is a reserved name", info.name);
	}
	string extension = "";
	if (FileSystem::IsRemoteFile(info.path, extension)) {
		if (!ExtensionHelper::TryAutoLoadExtension(context, extension)) {
			throw MissingExtensionException("Attaching path '%s' requires extension '%s' to be loaded", info.path,
			                                extension);
		}
		if (options.access_mode == AccessMode::AUTOMATIC) {
			// Attaching of remote files gets bumped to READ_ONLY
			// This is due to the fact that on most (all?) remote files writes to DB are not available
			// and having this raised later is not super helpful
			options.access_mode = AccessMode::READ_ONLY;
		}
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

	auto entry = databases->GetEntry(context, name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Failed to detach database with name \"%s\": database not found", name);
		}
		return;
	}
	auto &db = entry->Cast<AttachedDatabase>();
	db.OnDetach(context);

	if (!databases->DropEntry(context, name, false, true)) {
		throw InternalException("Failed to drop attached database");
	}
}

void DatabaseManager::CheckPathConflict(ClientContext &context, const string &path) {
	// Ensure that we did not already attach a database with the same path.
	string db_name = "";
	{
		lock_guard<mutex> path_lock(db_paths_lock);
		auto it = db_paths_to_name.find(path);
		if (it != db_paths_to_name.end()) {
			db_name = it->second;
		}
	}
	if (db_name.empty()) {
		return;
	}

	// Check against the catalog set.
	auto entry = GetDatabase(context, db_name);
	if (!entry) {
		return;
	}
	if (entry->IsSystem()) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(*entry);
	if (catalog.InMemory()) {
		return;
	}
	throw BinderException("Unique file handle conflict: Database \"%s\" is already attached with path \"%s\", ",
	                      db_name, path);
}

void DatabaseManager::InsertDatabasePath(ClientContext &context, const string &path, const string &name) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}

	CheckPathConflict(context, path);
	lock_guard<mutex> path_lock(db_paths_lock);
	db_paths_to_name[path] = name;
}

void DatabaseManager::EraseDatabasePath(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto path_it = db_paths_to_name.find(path);
	if (path_it != db_paths_to_name.end()) {
		db_paths_to_name.erase(path_it);
	}
}

vector<string> DatabaseManager::GetAttachedDatabasePaths() {
	lock_guard<mutex> path_lock(db_paths_lock);
	vector<string> paths;
	for (auto &entry : db_paths_to_name) {
		paths.push_back(entry.first);
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

	if (options.db_type.empty()) {
		return;
	}

	if (config.storage_extensions.find(options.db_type) != config.storage_extensions.end()) {
		// If the database type is already registered, we don't need to load it again.
		return;
	}

	// If we are loading a database type from an extension, then we need to check if that extension is loaded.
	if (!Catalog::TryAutoLoad(context, options.db_type)) {
		// FIXME: Here it might be preferable to use an AutoLoadOrThrow kind of function
		// so that either there will be success or a message to throw, and load will be
		// attempted only once respecting the auto-loading options
		ExtensionHelper::LoadExternalExtension(context, options.db_type);
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

vector<reference<AttachedDatabase>> DatabaseManager::GetDatabases(ClientContext &context,
                                                                  const optional_idx max_db_count) {
	vector<reference<AttachedDatabase>> result;
	idx_t count = 2;
	databases->ScanWithReturn(context, [&](CatalogEntry &entry) {
		if (max_db_count.IsValid() && count >= max_db_count.GetIndex()) {
			return false;
		}
		result.push_back(entry.Cast<AttachedDatabase>());
		count++;
		return true;
	});

	if (!max_db_count.IsValid() || max_db_count.GetIndex() >= 1) {
		result.push_back(*system);
	}
	if (!max_db_count.IsValid() || max_db_count.GetIndex() >= 2) {
		result.push_back(*context.client_data->temporary_objects);
	}

	return result;
}

vector<reference<AttachedDatabase>> DatabaseManager::GetDatabases() {
	vector<reference<AttachedDatabase>> result;
	databases->Scan([&](CatalogEntry &entry) { result.push_back(entry.Cast<AttachedDatabase>()); });
	result.push_back(*system);
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
