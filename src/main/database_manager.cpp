#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db) {
	system_catalog = make_unique<Catalog>(db);
}

DatabaseManager::~DatabaseManager() {
}

AttachedDatabase *DatabaseManager::GetDatabase(const string &name) {
	lock_guard<mutex> l(manager_lock);
	auto entry = databases.find(name);
	if (entry != databases.end()) {
		return entry->second.get();
	}
	return nullptr;
}

void DatabaseManager::AddDatabase(string name, unique_ptr<AttachedDatabase> catalog) {
	lock_guard<mutex> l(manager_lock);
	auto entry = databases.find(name);
	if (entry != databases.end()) {
		throw CatalogException("Catalog with name \"%s\" already exists", name);
	}
	databases[name] = move(catalog);
}

AttachedDatabase &DatabaseManager::GetDefaultDatabase() {
	lock_guard<mutex> l(manager_lock);
	for (auto &db : databases) {
		return *db.second;
	}
	throw InternalException("GetDefaultDatabase called but there are no databases");
}

Catalog &DatabaseManager::GetSystemCatalog() {
	return *system_catalog;
}

} // namespace duckdb
