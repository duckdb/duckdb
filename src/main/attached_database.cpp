#include "duckdb/main/attached_database.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type)
    : CatalogEntry(CatalogType::DATABASE_ENTRY,
                   type == AttachedDatabaseType::SYSTEM_DATABASE ? SYSTEM_CATALOG : TEMP_CATALOG, 0),
      db(db), type(type) {
	D_ASSERT(type == AttachedDatabaseType::TEMP_DATABASE || type == AttachedDatabaseType::SYSTEM_DATABASE);
	if (type == AttachedDatabaseType::TEMP_DATABASE) {
		storage = make_uniq<SingleFileStorageManager>(*this, ":memory:", false);
	}
	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db),
      type(access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
                                                : AttachedDatabaseType::READ_WRITE_DATABASE),
      parent_catalog(&catalog_p) {
	storage = make_uniq<SingleFileStorageManager>(*this, std::move(file_path_p), access_mode == AccessMode::READ_ONLY);
	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, StorageExtension &storage_extension,
                                   string name_p, AttachInfo &info, AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db),
      type(access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
                                                : AttachedDatabaseType::READ_WRITE_DATABASE),
      parent_catalog(&catalog_p) {
	catalog = storage_extension.attach(storage_extension.storage_info.get(), *this, name, info, access_mode);
	if (!catalog) {
		throw InternalException("AttachedDatabase - attach function did not return a catalog");
	}
	transaction_manager =
	    storage_extension.create_transaction_manager(storage_extension.storage_info.get(), *this, *catalog);
	if (!transaction_manager) {
		throw InternalException(
		    "AttachedDatabase - create_transaction_manager function did not return a transaction manager");
	}
	internal = true;
}

AttachedDatabase::~AttachedDatabase() {
	if (Exception::UncaughtException()) {
		return;
	}
	if (!storage) {
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

string AttachedDatabase::ExtractDatabaseName(const string &dbpath, FileSystem &fs) {
	if (dbpath.empty() || dbpath == ":memory:") {
		return "memory";
	}
	return fs.ExtractBaseName(dbpath);
}

void AttachedDatabase::Initialize() {
	if (IsSystem()) {
		catalog->Initialize(true);
	} else {
		catalog->Initialize(false);
	}
	if (storage) {
		storage->Initialize();
	}
}

StorageManager &AttachedDatabase::GetStorageManager() {
	if (!storage) {
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

Catalog &AttachedDatabase::ParentCatalog() {
	return *parent_catalog;
}

bool AttachedDatabase::IsInitialDatabase() const {
	return is_initial_database;
}

void AttachedDatabase::SetInitialDatabase() {
	is_initial_database = true;
}

} // namespace duckdb
