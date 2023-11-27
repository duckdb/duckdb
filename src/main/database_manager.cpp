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

DatabaseManager::DatabaseManager(DatabaseInstance &db) : catalog_version(0), current_query_number(1) {
	system = make_uniq<AttachedDatabase>(db);
	databases = make_uniq<CatalogSet>(system->GetCatalog());
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	system->Initialize();
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabase(ClientContext &context, const string &name) {
	if (StringUtil::Lower(name) == TEMP_CATALOG) {
		return context.client_data->temporary_objects.get();
	}
	return reinterpret_cast<AttachedDatabase *>(databases->GetEntry(context, name).get());
}

optional_ptr<AttachedDatabase> DatabaseManager::AttachDatabase(ClientContext &context, const AttachInfo &info,
                                                               const string &db_type, AccessMode access_mode) {
	// now create the attached database
	auto &db = DatabaseInstance::GetDatabase(context);
	auto attached_db = db.CreateAttachedDatabase(info, db_type, access_mode);

	InsertDatabasePath(info.path, attached_db->name);

	const auto name = attached_db->GetName();
	attached_db->oid = ModifyCatalog();
	DependencyList dependencies;
	if (default_database.empty()) {
		default_database = name;
	}

	// and add it to the databases catalog set
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

void DatabaseManager::InsertDatabasePath(const string &path, const string &name) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}

	lock_guard<mutex> write_lock(db_paths_lock);

	// ensure that we did not already attach a database with the same path
	if (db_paths.find(path) != db_paths.end()) {
		throw BinderException(
		    "Unique file handle conflict: Database \"%s\" is already attached with path \"%s\", "
		    "possibly by another transaction. Commit that transaction, if it already detached the file.",
		    name, path);
	}

	db_paths.insert(make_pair(path, name));
}

void DatabaseManager::EraseDatabasePath(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> write_lock(db_paths_lock);
	auto path_it = db_paths.find(path);
	if (path_it != db_paths.end()) {
		db_paths.erase(path_it);
	}
}

void DatabaseManager::GetDatabaseType(ClientContext &context, string &db_type, AttachInfo &info, const DBConfig &config,
                                      const string &unrecognized_option) {

	// duckdb database file
	if (StringUtil::CIEquals(db_type, "DUCKDB")) {
		db_type = "";

		// DUCKDB format does not allow unrecognized options
		if (!unrecognized_option.empty()) {
			throw BinderException("Unrecognized option for attach \"%s\"", unrecognized_option);
		}
		return;
	}

	// try to extract database type from path
	if (db_type.empty()) {
		lock_guard<mutex> write_lock(db_paths_lock);

		// we cannot infer the database type if we already hold the file handle somewhere else
		if (db_paths.find(info.path) != db_paths.end()) {
			throw BinderException(
			    "Unique file handle conflict: Database \"%s\" is already attached with path \"%s\", "
			    "possibly by another transaction. Commit that transaction, if it already detached the file. Otherwise, "
			    "inferring the database type from the file header is not possible, as it requires holding the file "
			    "handle.",
			    info.name, info.path);
		}

		DBPathAndType::CheckMagicBytes(info.path, db_type, config);
	}

	// if we are loading a database type from an extension - check if that extension is loaded
	if (!db_type.empty()) {
		if (!Catalog::TryAutoLoad(context, db_type)) {
			// FIXME: Here it might be preferable to use an AutoLoadOrThrow kind of function
			// so that either there will be success or a message to throw, and load will be
			// attempted only once respecting the auto-loading options
			ExtensionHelper::LoadExternalExtension(context, db_type);
		}
		return;
	}

	// DUCKDB format does not allow unrecognized options
	if (!unrecognized_option.empty()) {
		throw BinderException("Unrecognized option for attach \"%s\"", unrecognized_option);
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

void DatabaseManager::ResetDatabases() {
	databases.reset();
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
