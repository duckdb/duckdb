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
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/storage/block_allocator.hpp"

namespace duckdb {

StoredDatabasePath::StoredDatabasePath(DatabaseManager &db_manager, DatabaseFilePathManager &manager, string path_p,
                                       const string &name)
    : db_manager(db_manager), manager(manager), path(std::move(path_p)) {
}

StoredDatabasePath::~StoredDatabasePath() {
	manager.EraseDatabasePath(path);
}

void StoredDatabasePath::OnDetach() {
	manager.DetachDatabase(db_manager, path);
}

//===--------------------------------------------------------------------===//
// Attach Options
//===--------------------------------------------------------------------===//
AttachOptions::AttachOptions(const DBConfigOptions &options)
    : access_mode(options.access_mode), db_type(options.database_type) {
}

AttachOptions::AttachOptions(const unordered_map<string, Value> &attach_options, const AccessMode default_access_mode)
    : access_mode(default_access_mode) {
	for (auto &entry : attach_options) {
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

		if (entry.first == "recovery_mode") {
			// Extract the recovery mode.
			auto mode_str = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
			recovery_mode = EnumUtil::FromString<RecoveryMode>(mode_str);
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

		if (entry.first == "default_table") {
			default_table = QualifiedName::Parse(StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR)));
			continue;
		}
		options.emplace(entry.first, entry.second);
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
		unordered_map<string, Value> options;
		AttachOptions attach_options(options, AccessMode::READ_WRITE);
		storage = make_uniq<SingleFileStorageManager>(*this, string(IN_MEMORY_PATH), attach_options);
	}

	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p) {
	if (options.access_mode == AccessMode::READ_ONLY) {
		type = AttachedDatabaseType::READ_ONLY_DATABASE;
	} else {
		type = AttachedDatabaseType::READ_WRITE_DATABASE;
	}
	recovery_mode = options.recovery_mode;
	visibility = options.visibility;

	// We create the storage after the catalog to guarantee we allow extensions to instantiate the DuckCatalog.
	catalog = make_uniq<DuckCatalog>(*this);
	stored_database_path = std::move(options.stored_database_path);
	storage = make_uniq<SingleFileStorageManager>(*this, std::move(file_path_p), options);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, StorageExtension &storage_extension_p,
                                   ClientContext &context, string name_p, AttachInfo &info, AttachOptions &options)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db), parent_catalog(&catalog_p),
      storage_extension(&storage_extension_p) {
	if (options.access_mode == AccessMode::READ_ONLY) {
		type = AttachedDatabaseType::READ_ONLY_DATABASE;
	} else {
		type = AttachedDatabaseType::READ_WRITE_DATABASE;
	}
	recovery_mode = options.recovery_mode;
	visibility = options.visibility;

	optional_ptr<StorageExtensionInfo> storage_info = storage_extension->storage_info.get();
	catalog = storage_extension->attach(storage_info, context, *this, name, info, options);
	stored_database_path = std::move(options.stored_database_path);
	if (!catalog) {
		throw InternalException("AttachedDatabase - attach function did not return a catalog");
	}
	if (catalog->IsDuckCatalog()) {
		// The attached database uses the DuckCatalog.
		storage = make_uniq<SingleFileStorageManager>(*this, info.path, options);
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

string AttachedDatabase::StoredPath() const {
	if (stored_database_path) {
		return stored_database_path->path;
	}
	return string();
}

static string RemoveQueryParams(const string &name) {
	auto vec = StringUtil::Split(name, "?");
	D_ASSERT(!vec.empty());
	return vec[0];
}

string AttachedDatabase::ExtractDatabaseName(const string &dbpath, FileSystem &fs) {
	if (dbpath.empty() || dbpath == IN_MEMORY_PATH) {
		return "memory";
	}
	auto name = RemoveQueryParams(fs.ExtractBaseName(dbpath));
	if (NameIsReserved(name)) {
		name += "_db";
	}
	return name;
}

void AttachedDatabase::Initialize(optional_ptr<ClientContext> context) {
	if (IsSystem()) {
		catalog->Initialize(context, true);
	} else {
		catalog->Initialize(context, false);
	}
	if (storage) {
		storage->Initialize(context);
	}
}

void AttachedDatabase::FinalizeLoad(optional_ptr<ClientContext> context) {
	catalog->FinalizeLoad(context);
}

bool AttachedDatabase::HasStorageManager() const {
	return storage.get();
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

void AttachedDatabase::OnDetach(ClientContext &context) {
	if (catalog) {
		catalog->OnDetach(context);
	}
	if (stored_database_path && visibility != AttachVisibility::HIDDEN) {
		stored_database_path->OnDetach();
	}
}

void AttachedDatabase::Close() {
	if (is_closed) {
		return;
	}
	D_ASSERT(catalog);
	is_closed = true;

	// shutting down: attempt to checkpoint the database
	// but only if we are not cleaning up as part of an exception unwind
	if (!Exception::UncaughtException() && storage && !ValidChecker::IsInvalidated(db)) {
		if (!storage->InMemory()) {
			try {
				auto &config = DBConfig::GetConfig(db);
				if (config.options.checkpoint_on_shutdown) {
					CheckpointOptions options;
					options.wal_action = CheckpointWALAction::DELETE_WAL;
					storage->CreateCheckpoint(QueryContext(), options);
				}
			} catch (std::exception &ex) {
				ErrorData data(ex);
				try {
					DUCKDB_LOG_ERROR(db, "AttachedDatabase::Close()\t\t" + data.Message());
				} catch (...) { // NOLINT
				}
			} catch (...) { // NOLINT
			}
		}
		try {
			// destroy the storage
			storage->Destroy();
		} catch (...) { // NOLINT
		}
	}

	transaction_manager.reset();
	catalog.reset();
	storage.reset();
	stored_database_path.reset();
}

} // namespace duckdb
