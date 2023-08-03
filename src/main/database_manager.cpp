#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"

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

void DatabaseManager::AddDatabase(ClientContext &context, unique_ptr<AttachedDatabase> db_instance) {
	auto name = db_instance->GetName();
	db_instance->oid = ModifyCatalog();
	DependencyList dependencies;
	if (default_database.empty()) {
		default_database = name;
	}
	if (!databases->CreateEntry(context, name, std::move(db_instance), dependencies)) {
		throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
	}
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
	auto databases = GetDatabases(context);
	for (auto &db_ref : databases) {
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

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
