#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db) : db(db) {
	auto &config = DBConfig::GetConfig(db);
	this->name = ExtractDatabaseName(config.options.database_path);
	storage = make_unique<SingleFileStorageManager>(db, config.options.database_path,
	                                                config.options.access_mode == AccessMode::READ_ONLY);
	catalog = make_unique<Catalog>(db);
	transaction_manager = make_unique<TransactionManager>(db);
}

AttachedDatabase::~AttachedDatabase() {
}

string AttachedDatabase::ExtractDatabaseName(const string &dbpath) {
	if (dbpath.empty() || dbpath == ":memory:") {
		return "memory";
	}
	return FileSystem::ExtractBaseName(dbpath);
}

void AttachedDatabase::Initialize() {
	catalog->Initialize(false);
	storage->Initialize();
}

StorageManager &AttachedDatabase::GetStorageManager() {
	return *storage;
}

Catalog &AttachedDatabase::GetCatalog() {
	return *catalog;
}

TransactionManager &AttachedDatabase::GetTransactionManager() {
	return *transaction_manager;
}

} // namespace duckdb
