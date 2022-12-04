#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db) : db(db), name("system") {
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, string name_p, string file_path, AccessMode access_mode) :
    db(db), name(move(name_p)) {
	storage = make_unique<SingleFileStorageManager>(*this, file_path, access_mode == AccessMode::READ_ONLY);
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
}


AttachedDatabase::~AttachedDatabase() {
	if (Exception::UncaughtException()) {
		return;
	}
	if (IsSystem()) {
		return;
	}

	// shutting down: attempt to checkpoint the database
	// but only if we are not cleaning up as part of an exception unwind
	try {
		if (!storage->InMemory()) {
			auto &config = DBConfig::GetConfig(db);
			if (!config.options.checkpoint_on_shutdown) {
				return;
			}
			storage->CreateCheckpoint(true);
		}
	} catch (...) {
	}
}

bool AttachedDatabase::IsSystem() const {
	return !storage;
}

string AttachedDatabase::ExtractDatabaseName(const string &dbpath) {
	if (dbpath.empty() || dbpath == ":memory:") {
		return "memory";
	}
	return FileSystem::ExtractBaseName(dbpath);
}

void AttachedDatabase::Initialize() {
	if (IsSystem()) {
		catalog->Initialize(true);
	} else {
		catalog->Initialize(false);
		storage->Initialize();
	}
}

StorageManager &AttachedDatabase::GetStorageManager() {
	if (IsSystem()) {
		throw InternalException("Internal system catalog does not have storage");
	}
	return *storage;
}

Catalog &AttachedDatabase::GetCatalog() {
	return *catalog;
}

TransactionManager &AttachedDatabase::GetTransactionManager() {
	return *transaction_manager;
}

} // namespace duckdb
