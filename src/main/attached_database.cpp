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

AttachOptions::AttachOptions(const AccessMode access_mode, const string &db_type)
    : access_mode(access_mode), db_type(db_type), block_alloc_size(DEFAULT_BLOCK_ALLOC_SIZE) {
}

AttachOptions::AttachOptions(const unique_ptr<AttachInfo> &info, AccessMode access_mode_p)
    : access_mode(access_mode_p), block_alloc_size(DEFAULT_BLOCK_ALLOC_SIZE) {

	for (auto &entry : info->options) {

		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "readwrite" || entry.first == "read_write") {
			auto read_write = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (!read_write) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
			continue;
		}

		if (entry.first == "type") {
			// extract the database type
			db_type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
			continue;
		}

		if (entry.first == "block_size") {
			block_alloc_size = UBigIntValue::Get(entry.second.DefaultCastAs(LogicalType::UBIGINT));
			if (!IsPowerOfTwo(block_alloc_size)) {
				throw InvalidInputException("the block size must be a power of two, got %llu", block_alloc_size);
			}
			if (block_alloc_size < MIN_BLOCK_ALLOC_SIZE) {
				throw InvalidInputException(
				    "the block size must be greater or equal than the minimum block size of %llu, got %llu",
				    MIN_BLOCK_ALLOC_SIZE, block_alloc_size);
			}
			if (block_alloc_size != DEFAULT_BLOCK_ALLOC_SIZE) {
				throw NotImplementedException(
				    "other block sizes than the default block size are not supported, expected %llu, got %llu",
				    DEFAULT_BLOCK_ALLOC_SIZE, block_alloc_size);
			}
			continue;
		}

		// we allow unrecognized options
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

	// this database does not have storage - we default to the default block allocation size
	D_ASSERT(type == AttachedDatabaseType::TEMP_DATABASE || type == AttachedDatabaseType::SYSTEM_DATABASE);
	if (type == AttachedDatabaseType::TEMP_DATABASE) {
		storage = make_uniq<SingleFileStorageManager>(*this, string(IN_MEMORY_PATH), false, DEFAULT_BLOCK_ALLOC_SIZE);
	}

	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   const AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p) {

	type = options.access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
	                                                    : AttachedDatabaseType::READ_WRITE_DATABASE;
	catalog = make_uniq<DuckCatalog>(*this);
	// create the storage after the catalog to guarantee we allow extensions to instantiate the DuckCatalog
	storage = make_uniq<SingleFileStorageManager>(
	    *this, std::move(file_path_p), options.access_mode == AccessMode::READ_ONLY, options.block_alloc_size);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, StorageExtension &storage_extension,
                                   ClientContext &context, string name_p, const AttachInfo &info,
                                   const AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p) {

	type = options.access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
	                                                    : AttachedDatabaseType::READ_WRITE_DATABASE;
	catalog = storage_extension.attach(storage_extension.storage_info.get(), context, *this, name, *info.Copy(),
	                                   options.access_mode);
	if (!catalog) {
		throw InternalException("AttachedDatabase - attach function did not return a catalog");
	}
	if (catalog->IsDuckCatalog()) {
		// DuckCatalog, instantiate storage
		storage = make_uniq<SingleFileStorageManager>(*this, info.path, options.access_mode == AccessMode::READ_ONLY,
		                                              options.block_alloc_size);
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
	D_ASSERT(catalog);

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
	if (dbpath.empty() || dbpath == IN_MEMORY_PATH) {
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
