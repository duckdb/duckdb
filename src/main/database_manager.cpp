#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db)
    : catalog_version(0), current_query_number(1), default_database(nullptr) {
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
	unordered_set<CatalogEntry *> dependencies;
	if (!default_database) {
		default_database = db_instance.get();
	}
	if (!databases->CreateEntry(context, name, move(db_instance),
	                            dependencies)) {
		throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
	}
}

AttachedDatabase &DatabaseManager::GetDefaultDatabase() {
	if (!default_database) {
		throw InternalException("GetDefaultDatabase called but there are no databases");
	}
	return *default_database;
}

void DatabaseManager::SetDefaultDatabase(ClientContext &context, const string &name) {
	auto entry = (AttachedDatabase *)databases->GetEntry(context, name);
	if (!entry) {
		throw CatalogException("Database with name \"%s\" does not exist", name);
	}
	default_database = entry;
}

vector<AttachedDatabase *> DatabaseManager::GetDatabases(ClientContext &context) {
	vector<AttachedDatabase *> result;
	databases->Scan(context,
	                [&](CatalogEntry *entry) { result.push_back((AttachedDatabase *)entry); });
	return result;
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
