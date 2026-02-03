#include "duckdb/main/database_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db)
    : db(db), next_oid(0), current_query_number(1), current_transaction_id(0) {
	system = make_shared_ptr<AttachedDatabase>(db);
	auto &config = DBConfig::GetConfig(db);
	path_manager = config.path_manager;
	if (!path_manager) {
		// no shared path manager
		path_manager = make_shared_ptr<DatabaseFilePathManager>();
	}
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	// The SYSTEM_DATABASE has no persistent storage.
	system->Initialize();
}

void DatabaseManager::FinalizeStartup() {
	auto dbs = GetDatabases();
	for (auto &db : dbs) {
		db->FinalizeLoad(nullptr);
	}
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabase(ClientContext &context, const string &name) {
	auto &meta_transaction = MetaTransaction::Get(context);
	// first check if we have a local reference to this database already
	auto database = meta_transaction.GetReferencedDatabase(name);
	if (database) {
		// we do! return it
		return database;
	}
	lock_guard<mutex> guard(databases_lock);
	shared_ptr<AttachedDatabase> db;
	if (StringUtil::Lower(name) == TEMP_CATALOG) {
		db = context.client_data->temporary_objects;
	} else {
		db = GetDatabaseInternal(guard, name);
	}
	if (!db) {
		return nullptr;
	}
	return meta_transaction.UseDatabase(db);
}

shared_ptr<AttachedDatabase> DatabaseManager::GetDatabase(const string &name) {
	lock_guard<mutex> guard(databases_lock);
	return GetDatabaseInternal(guard, name);
}

shared_ptr<AttachedDatabase> DatabaseManager::GetDatabaseInternal(const lock_guard<mutex> &, const string &name) {
	if (StringUtil::Lower(name) == SYSTEM_CATALOG) {
		return system;
	}
	auto entry = databases.find(name);
	if (entry == databases.end()) {
		// not found
		return nullptr;
	}
	return entry->second;
}

bool RequiresTrackingAttaches(const string &path, const string &db_type) {
	// we need to track attaches for file-based duckdb databases
	if (!db_type.empty() && !StringUtil::CIEquals(db_type, "duckdb")) {
		// not duckdb - don't track
		return false;
	}
	if (path.empty() || path == IN_MEMORY_PATH) {
		// in-memory - don't track
		return false;
	}
	// file-based duckdb - track
	return true;
}

shared_ptr<AttachedDatabase> DatabaseManager::AttachDatabase(ClientContext &context, AttachInfo &info,
                                                             AttachOptions &options) {
	string extension = "";
	if (FileSystem::IsRemoteFile(info.path, extension)) {
		if (options.access_mode == AccessMode::AUTOMATIC) {
			// Attaching of remote files gets bumped to READ_ONLY
			// This is due to the fact that on most (all?) remote files writes to DB are not available
			// and having this raised later is not super helpful
			options.access_mode = AccessMode::READ_ONLY;
		}
	}
	bool requires_tracking_attaches = RequiresTrackingAttaches(info.path, options.db_type);
	if (requires_tracking_attaches) {
		// canonicalize the path to the database
		auto &fs = FileSystem::GetFileSystem(context);
		info.path = fs.CanonicalizePath(info.path);
	}

	// for IGNORE / REPLACE ON CONFLICT - first look for an existing entry
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// constant-time lookup in the catalog for the db name
		auto existing_db = GetDatabase(info.name);
		if (existing_db) {
			if ((existing_db->IsReadOnly() && options.access_mode == AccessMode::READ_WRITE) ||
			    (!existing_db->IsReadOnly() && options.access_mode == AccessMode::READ_ONLY)) {
				auto existing_mode = existing_db->IsReadOnly() ? AccessMode::READ_ONLY : AccessMode::READ_WRITE;
				auto existing_mode_str = EnumUtil::ToString(existing_mode);
				auto attached_mode = EnumUtil::ToString(options.access_mode);
				throw BinderException("Database \"%s\" is already attached in %s mode, cannot re-attach in %s mode",
				                      info.name, existing_mode_str, attached_mode);
			}
			if (!options.default_table.name.empty()) {
				existing_db->GetCatalog().SetDefaultTable(options.default_table.schema, options.default_table.name);
			}
			if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
				// allow custom catalogs to override this behavior
				if (!existing_db->GetCatalog().HasConflictingAttachOptions(info.path, options)) {
					return existing_db;
				}
			} else {
				return existing_db;
			}
		}
	}

	if (requires_tracking_attaches) {
		// Start timing the ATTACH-delay step.
		auto profiler = context.client_data->profiler->StartTimer(MetricType::WAITING_TO_ATTACH_LATENCY);

		while (InsertDatabasePath(info, options) == InsertDatabasePathResult::ALREADY_EXISTS) {
			// database with this name and path already exists
			// first check if it exists within this transaction
			auto &meta_transaction = MetaTransaction::Get(context);
			auto existing_db = meta_transaction.GetReferencedDatabaseOwning(info.name);
			if (existing_db) {
				// it does! return it
				return existing_db;
			}

			// ... but it might not be done attaching yet!
			// verify the database has actually finished attaching prior to returning
			lock_guard<mutex> guard(databases_lock);
			auto entry = databases.find(info.name);
			if (entry != databases.end()) {
				// The database ACTUALLY exists, so we return it.
				return entry->second;
			}
			context.InterruptCheck();
		}
	}
	auto &config = DBConfig::GetConfig(context);
	GetDatabaseType(context, info, config, options);
	if (!options.db_type.empty()) {
		// we only need to prevent duplicate opening of DuckDB files
		// if this is not a DuckDB file but e.g. a CSV or Parquet file, we don't need to do this duplicate protection
		options.stored_database_path.reset();
	}
	if (AttachedDatabase::NameIsReserved(info.name)) {
		throw BinderException("Attached database name \"%s\" cannot be used because it is a reserved name", info.name);
	}
	if (!extension.empty()) {
		if (!ExtensionHelper::TryAutoLoadExtension(context, extension)) {
			throw MissingExtensionException("Attaching path '%s' requires extension '%s' to be loaded", info.path,
			                                extension);
		}
	}

	// now create the attached database
	auto &db = DatabaseInstance::GetDatabase(context);
	auto attached_db = db.CreateAttachedDatabase(context, info, options);

	//! Initialize the database.
	if (options.is_main_database) {
		attached_db->SetInitialDatabase();
		attached_db->Initialize(context);
	} else {
		attached_db->Initialize(context);
		if (!options.default_table.name.empty()) {
			attached_db->GetCatalog().SetDefaultTable(options.default_table.schema, options.default_table.name);
		}
		attached_db->FinalizeLoad(context);
	}

	FinalizeAttach(context, info, attached_db);
	return attached_db;
}

optional_ptr<AttachedDatabase> DatabaseManager::FinalizeAttach(ClientContext &context, AttachInfo &info,
                                                               shared_ptr<AttachedDatabase> attached_db) {
	const auto name = attached_db->GetName();
	attached_db->oid = NextOid();
	if (default_database.empty()) {
		default_database = name;
	}
	shared_ptr<AttachedDatabase> detached_db;
	{
		lock_guard<mutex> guard(databases_lock);
		auto entry = databases.emplace(name, attached_db);
		if (!entry.second) {
			if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
				// override existing entry
				detached_db = std::move(entry.first->second);
				databases[name] = attached_db;
			} else {
				throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
			}
		}
	}
	auto &meta_transaction = MetaTransaction::Get(context);
	if (detached_db) {
		meta_transaction.DetachDatabase(*detached_db);
		detached_db->OnDetach(context);
		detached_db.reset();
	}
	auto &db_ref = meta_transaction.UseDatabase(attached_db);
	auto &transaction = DuckTransaction::Get(context, *system);
	auto &transaction_manager = DuckTransactionManager::Get(*system);
	transaction_manager.PushAttach(transaction, db_ref);
	return db_ref;
}

void DatabaseManager::DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found) {
	if (GetDefaultDatabase(context) == name) {
		throw BinderException("Cannot detach database \"%s\" because it is the default database. Select a different "
		                      "database using `USE` to allow detaching this database",
		                      name);
	}

	auto attached_db = DetachInternal(name);
	if (!attached_db) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Failed to detach database with name \"%s\": database not found", name);
		}
		return;
	}

	attached_db->OnDetach(context);

	// DetachInternal removes the AttachedDatabase from the list of databases that can be referenced.
	AttachedDatabase::InvokeCloseIfLastReference(attached_db);
}

void DatabaseManager::Alter(ClientContext &context, AlterInfo &info) {
	auto &db_info = info.Cast<AlterDatabaseInfo>();

	switch (db_info.alter_database_type) {
	case AlterDatabaseType::RENAME_DATABASE: {
		auto &rename_info = db_info.Cast<RenameDatabaseInfo>();
		RenameDatabase(context, db_info.catalog, rename_info.new_name, db_info.if_not_found);
		break;
	}
	default:
		throw InternalException("Unsupported ALTER DATABASE operation");
	}
}

void DatabaseManager::RenameDatabase(ClientContext &context, const string &old_name, const string &new_name,
                                     OnEntryNotFound if_not_found) {
	if (AttachedDatabase::NameIsReserved(new_name)) {
		throw BinderException("Database name \"%s\" cannot be used because it is a reserved name", new_name);
	}

	shared_ptr<AttachedDatabase> attached_db;
	{
		lock_guard<mutex> guard(databases_lock);
		auto old_entry = databases.find(old_name);
		if (old_entry == databases.end()) {
			if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
				throw BinderException("Failed to rename database \"%s\": database not found", old_name);
			}
			return;
		}

		auto new_entry = databases.find(new_name);
		if (new_entry != databases.end()) {
			throw BinderException("Failed to rename database \"%s\" to \"%s\": database with new name already exists",
			                      old_name, new_name);
		}

		attached_db = old_entry->second;
		databases.erase(old_entry);
		attached_db->SetName(new_name);
		databases[new_name] = attached_db;
	}

	if (default_database == old_name) {
		default_database = new_name;
	}
}

shared_ptr<AttachedDatabase> DatabaseManager::DetachInternal(const string &name) {
	shared_ptr<AttachedDatabase> attached_db;
	{
		lock_guard<mutex> guard(databases_lock);
		auto entry = databases.find(name);
		if (entry == databases.end()) {
			return nullptr;
		}
		attached_db = std::move(entry->second);
		databases.erase(entry);
	}
	return attached_db;
}

idx_t DatabaseManager::ApproxDatabaseCount() {
	return path_manager->ApproxDatabaseCount();
}

InsertDatabasePathResult DatabaseManager::InsertDatabasePath(const AttachInfo &info, AttachOptions &options) {
	return path_manager->InsertDatabasePath(*this, info.path, info.name, info.on_conflict, options);
}

vector<string> DatabaseManager::GetAttachedDatabasePaths() {
	vector<string> result;
	lock_guard<mutex> guard(databases_lock);
	for (auto &entry : databases) {
		auto &db_ref = *entry.second;
		auto &catalog = db_ref.GetCatalog();
		if (catalog.InMemory() || catalog.IsSystemCatalog()) {
			continue;
		}
		auto path = catalog.GetDBPath();
		if (path.empty()) {
			continue;
		}
		result.push_back(std::move(path));
	}
	return result;
}

void DatabaseManager::GetDatabaseType(ClientContext &context, AttachInfo &info, const DBConfig &config,
                                      AttachOptions &options) {
	// Test if the database is a DuckDB database file.
	if (StringUtil::CIEquals(options.db_type, "duckdb")) {
		options.db_type = "";
		return;
	}

	// Try to extract the database type from the path.
	if (options.db_type.empty()) {
		auto &fs = FileSystem::GetFileSystem(context);
		DBPathAndType::CheckMagicBytes(context, fs, info.path, options.db_type);
	}

	if (options.db_type.empty()) {
		return;
	}

	auto extension_name = ExtensionHelper::ApplyExtensionAlias(options.db_type);
	if (StorageExtension::Find(config, extension_name)) {
		// If the database type is already registered, we don't need to load it again.
		return;
	}

	// If we are loading a database type from an extension, then we need to check if that extension is loaded.
	if (!Catalog::TryAutoLoad(context, options.db_type)) {
		// FIXME: Here it might be preferable to use an AutoLoadOrThrow kind of function
		// so that either there will be success or a message to throw, and load will be
		// attempted only once respecting the auto-loading options
		ExtensionHelper::LoadExternalExtension(context, options.db_type);
	}
}

const string &DatabaseManager::GetDefaultDatabase(ClientContext &context) {
	auto &config = ClientData::Get(context);
	auto &default_entry = config.catalog_search_path->GetDefault();
	if (IsInvalidCatalog(default_entry.catalog)) {
		auto &result = DatabaseManager::Get(context).default_database;
		if (result.empty()) {
			throw InternalException("Calling DatabaseManager::GetDefaultDatabase with no default database set");
		}
		return result;
	}
	return default_entry.catalog;
}

// LCOV_EXCL_START
void DatabaseManager::SetDefaultDatabase(ClientContext &context, const string &new_value) {
	auto db_entry = GetDatabase(context, new_value);

	if (!db_entry) {
		throw InternalException("Database \"%s\" not found", new_value);
	} else if (db_entry->IsTemporary()) {
		throw InternalException("Cannot set the default database to a temporary database");
	} else if (db_entry->IsSystem()) {
		throw InternalException("Cannot set the default database to a system database");
	}

	default_database = new_value;
}
// LCOV_EXCL_STOP

vector<shared_ptr<AttachedDatabase>> DatabaseManager::GetDatabases(ClientContext &context,
                                                                   const optional_idx max_db_count) {
	vector<shared_ptr<AttachedDatabase>> result;

	lock_guard<mutex> guard(databases_lock);
	idx_t count = 2;
	for (auto &entry : databases) {
		if (max_db_count.IsValid() && count >= max_db_count.GetIndex()) {
			break;
		}
		result.push_back(entry.second);
		count++;
	}
	if (!max_db_count.IsValid() || max_db_count.GetIndex() >= 1) {
		result.push_back(system);
	}
	if (!max_db_count.IsValid() || max_db_count.GetIndex() >= 2) {
		result.push_back(context.client_data->temporary_objects);
	}

	return result;
}

vector<shared_ptr<AttachedDatabase>> DatabaseManager::GetDatabases() {
	vector<shared_ptr<AttachedDatabase>> result;

	lock_guard<mutex> guard(databases_lock);
	for (auto &entry : databases) {
		result.push_back(entry.second);
	}
	result.push_back(system);
	return result;
}

void DatabaseManager::ResetDatabases() {
	auto shared_db_pointers = GetDatabases();
	for (auto &entry : shared_db_pointers) {
		entry->Close(DatabaseCloseAction::TRY_CHECKPOINT);
		entry.reset();
	}
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb
