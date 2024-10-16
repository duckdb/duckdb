#include "duckdb/main/attached_database.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Attach Options
//===--------------------------------------------------------------------===//

AttachOptions::AttachOptions(const DBConfigOptions &options)
    : access_mode(options.access_mode), db_type(options.database_type) {
}

AttachOptions::AttachOptions(const unique_ptr<AttachInfo> &info, const AccessMode default_access_mode)
    : access_mode(default_access_mode) {

	for (auto &entry : info->options) {

		if (entry.first == "readonly" || entry.first == "read_only") {
			// Extract the read access mode.

			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "readwrite" || entry.first == "read_write") {
			// Extract the write access mode.

			auto read_write = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (!read_write) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "type") {
			// Extract the database type.
			db_type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
			continue;
		}

		// We allow unrecognized options in storage extensions. To track that we saw an unrecognized option,
		// we set unrecognized_option.
		if (unrecognized_option.empty()) {
			unrecognized_option = entry.first;
		}
	}
}

//===--------------------------------------------------------------------===//
// Attached Database
//===--------------------------------------------------------------------===//

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type)
    : CatalogEntry(CatalogType::DATABASE_ENTRY,
                   type == AttachedDatabaseType::SYSTEM_DATABASE ? SYSTEM_CATALOG : TEMP_CATALOG, 0),
      db(db), type(type) {

	// This database does not have storage, or uses temporary_objects for in-memory storage.
	D_ASSERT(type == AttachedDatabaseType::TEMP_DATABASE || type == AttachedDatabaseType::SYSTEM_DATABASE);
	if (type == AttachedDatabaseType::TEMP_DATABASE) {
		storage = make_uniq<SingleFileStorageManager>(*this, string(IN_MEMORY_PATH), false);
	}

	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   const AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p) {

	if (options.access_mode == AccessMode::READ_ONLY) {
		type = AttachedDatabaseType::READ_ONLY_DATABASE;
	} else {
		type = AttachedDatabaseType::READ_WRITE_DATABASE;
	}

	// We create the storage after the catalog to guarantee we allow extensions to instantiate the DuckCatalog.
	catalog = make_uniq<DuckCatalog>(*this);
	auto read_only = options.access_mode == AccessMode::READ_ONLY;
	storage = make_uniq<SingleFileStorageManager>(*this, std::move(file_path_p), read_only);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, StorageExtension &storage_extension_p,
                                   ClientContext &context, string name_p, const AttachInfo &info,
                                   const AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p),
      storage_extension(&storage_extension_p) {

	if (options.access_mode == AccessMode::READ_ONLY) {
		type = AttachedDatabaseType::READ_ONLY_DATABASE;
	} else {
		type = AttachedDatabaseType::READ_WRITE_DATABASE;
	}

	StorageExtensionInfo *storage_info = storage_extension->storage_info.get();
	catalog = storage_extension->attach(storage_info, context, *this, name, *info.Copy(), options.access_mode);
	if (!catalog) {
		throw InternalException("AttachedDatabase - attach function did not return a catalog");
	}
	if (catalog->IsDuckCatalog()) {
		// The attached database uses the DuckCatalog.
		auto read_only = options.access_mode == AccessMode::READ_ONLY;
		storage = make_uniq<SingleFileStorageManager>(*this, info.path, read_only);
	}
	transaction_manager = storage_extension->create_transaction_manager(storage_info, *this, *catalog);
	if (!transaction_manager) {
		throw InternalException(
		    "AttachedDatabase - create_transaction_manager function did not return a transaction manager");
	}
	internal = true;
}

AttachedDatabase::~AttachedDatabase() {
	Close();
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

bool AttachedDatabase::NameIsReserved(const string &name) {
	return name == DEFAULT_SCHEMA || name == TEMP_CATALOG || name == SYSTEM_CATALOG;
}

string AttachedDatabase::ExtractDatabaseName(const string &dbpath, FileSystem &fs) {
	if (dbpath.empty() || dbpath == IN_MEMORY_PATH) {
		return "memory";
	}
	auto name = fs.ExtractBaseName(dbpath);
	if (NameIsReserved(name)) {
		name += "_db";
	}
	return name;
}

void AttachedDatabase::Initialize(const optional_idx block_alloc_size) {
	if (IsSystem()) {
		catalog->Initialize(true);
	} else {
		catalog->Initialize(false);
	}
	if (storage) {
		storage->Initialize(block_alloc_size);
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

const Catalog &AttachedDatabase::ParentCatalog() const {
	return *parent_catalog;
}

bool AttachedDatabase::IsInitialDatabase() const {
	return is_initial_database;
}

void AttachedDatabase::SetInitialDatabase() {
	is_initial_database = true;
}

void AttachedDatabase::SetReadOnlyDatabase() {
	type = AttachedDatabaseType::READ_ONLY_DATABASE;
}

void AttachedDatabase::Close() {
	D_ASSERT(catalog);
	if (is_closed) {
		return;
	}
	is_closed = true;

	if (!IsSystem() && !catalog->InMemory()) {
		db.GetDatabaseManager().EraseDatabasePath(catalog->GetDBPath());
	}

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
			CheckpointOptions options;
			options.wal_action = CheckpointWALAction::DELETE_WAL;
			storage->CreateCheckpoint(options);
		}
	} catch (...) { // NOLINT
	}

	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

} // namespace duckdb
