#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db) : catalog_version(0), current_query_number(1) {
	system = make_unique<AttachedDatabase>(db);
	databases = make_unique<CatalogSet>(system->GetCatalog());
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	system->Initialize();
}

AttachedDatabase *DatabaseManager::GetDatabase(ClientContext &context, const string &name) {
	if (StringUtil::Lower(name) == TEMP_CATALOG) {
		return context.client_data->temporary_objects.get();
	}
	return (AttachedDatabase *)databases->GetEntry(context, name);
}

void DatabaseManager::AddDatabase(ClientContext &context, unique_ptr<AttachedDatabase> db_instance) {
	auto name = db_instance->GetName();
	DependencyList dependencies;
	if (default_database.empty()) {
		default_database = name;
	}
	if (!databases->CreateEntry(context, name, move(db_instance), dependencies)) {
		throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
	}
}

void DatabaseManager::DetachDatabase(ClientContext &context, const string &name, bool if_exists) {
	if (!databases->DropEntry(context, name, false, true)) {
		if (!if_exists) {
			throw BinderException("Failed to detach database with name \"%s\": database not found", name);
		}
	}
}

AttachedDatabase *DatabaseManager::GetDatabaseFromPath(ClientContext &context, const string &path) {
	auto databases = GetDatabases(context);
	for (auto db : databases) {
		if (db->IsSystem()) {
			continue;
		}
		auto &storage = db->GetStorageManager();
		if (storage.InMemory()) {
			continue;
		}
		if (StringUtil::CIEquals(path, storage.GetDBPath())) {
			return db;
		}
	}
	return nullptr;
}

const string &DatabaseManager::GetDefaultDatabase(ClientContext &context) {
	auto &config = ClientData::Get(context);
	auto &default_entry = config.catalog_search_path->GetDefault();
	if (default_entry.catalog == INVALID_CATALOG) {
		auto &result = DatabaseManager::Get(context).default_database;
		if (result.empty()) {
			throw InternalException("Calling DatabaseManager::GetDefaultDatabase with no default database set");
		}
		return result;
	}
	return default_entry.catalog;
}

vector<AttachedDatabase *> DatabaseManager::GetDatabases(ClientContext &context) {
	vector<AttachedDatabase *> result;
	databases->Scan(context, [&](CatalogEntry *entry) { result.push_back((AttachedDatabase *)entry); });
	result.push_back(system.get());
	result.push_back(context.client_data->temporary_objects.get());
	return result;
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
