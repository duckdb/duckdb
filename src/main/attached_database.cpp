#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, nullptr,
                   type == AttachedDatabaseType::SYSTEM_DATABASE ? SYSTEM_CATALOG : TEMP_CATALOG),
      db(db), type(type) {
	D_ASSERT(type == AttachedDatabaseType::TEMP_DATABASE || type == AttachedDatabaseType::SYSTEM_DATABASE);
	if (type == AttachedDatabaseType::TEMP_DATABASE) {
		storage = make_unique<SingleFileStorageManager>(*this, ":memory:", false);
	}
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, &catalog_p, std::move(name_p)), db(db),
      type(access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
                                                : AttachedDatabaseType::READ_WRITE_DATABASE) {
	storage = make_unique<SingleFileStorageManager>(*this, std::move(file_path_p), access_mode == AccessMode::READ_ONLY);
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
	D_ASSERT(!storage || type != AttachedDatabaseType::SYSTEM_DATABASE);
	return type == AttachedDatabaseType::SYSTEM_DATABASE;
}

bool AttachedDatabase::IsTemporary() const {
	return type == AttachedDatabaseType::TEMP_DATABASE;
}
bool AttachedDatabase::IsReadOnly() const {
	return type == AttachedDatabaseType::READ_ONLY_DATABASE;
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
