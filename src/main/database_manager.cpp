#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db) : catalog_version(0), current_query_number(1) {
	system = make_unique<AttachedDatabase>(db);
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	system->Initialize();
}

AttachedDatabase *DatabaseManager::GetDatabase(const string &name) {
	lock_guard<mutex> l(manager_lock);
	auto entry = databases.find(name);
	if (entry != databases.end()) {
		return entry->second.get();
	}
	return nullptr;
}

void DatabaseManager::AddDatabase(unique_ptr<AttachedDatabase> db_instance) {
	lock_guard<mutex> l(manager_lock);
	auto &name = db_instance->GetName();
	auto entry = databases.find(name);
	if (entry != databases.end()) {
		throw CatalogException("Catalog with name \"%s\" already exists", name);
	}
	databases[name] = move(db_instance);
}

AttachedDatabase &DatabaseManager::GetDefaultDatabase() {
	lock_guard<mutex> l(manager_lock);
	for (auto &db : databases) {
		return *db.second;
	}
	throw InternalException("GetDefaultDatabase called but there are no databases");
}

vector<AttachedDatabase *> DatabaseManager::GetDatabases() {
	lock_guard<mutex> l(manager_lock);
	vector<AttachedDatabase *> result;
	for (auto &entry : databases) {
		result.push_back(entry.second.get());
	}
	return result;
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
