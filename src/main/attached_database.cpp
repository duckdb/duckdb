#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, BuiltInDatabaseType type)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, nullptr,
                   type == BuiltInDatabaseType::SYSTEM_DATABASE ? "system" : "temp"),
      db(db), type(type) {
	D_ASSERT(type == BuiltInDatabaseType::TEMP_DATABASE || type == BuiltInDatabaseType::SYSTEM_DATABASE);
	if (type == BuiltInDatabaseType::TEMP_DATABASE) {
		storage = make_unique<SingleFileStorageManager>(*this, ":memory:", false);
	}
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path,
                                   AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, &catalog_p, move(name_p)), db(db),
      type(BuiltInDatabaseType::NOT_BUILT_IN) {
	storage = make_unique<SingleFileStorageManager>(*this, file_path, access_mode == AccessMode::READ_ONLY);
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
	internal = true;
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
	D_ASSERT(!storage || type != BuiltInDatabaseType::SYSTEM_DATABASE);
	return type == BuiltInDatabaseType::SYSTEM_DATABASE;
}

bool AttachedDatabase::IsTemporary() const {
	return type == BuiltInDatabaseType::TEMP_DATABASE;
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
