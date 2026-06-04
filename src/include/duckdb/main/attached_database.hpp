//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/attached_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {
class Catalog;
class DatabaseInstance;
class StorageManager;
class TransactionManager;
class StorageExtension;
class DatabaseManager;

struct AttachInfo;
struct StoredDatabasePath;

enum class AttachedDatabaseType {
	READ_WRITE_DATABASE,
	READ_ONLY_DATABASE,
	SYSTEM_DATABASE,
	TEMP_DATABASE,
};

//! Controls whether the attached database provides any tables, and whether it surfaces in
//! catalog enumeration (`SHOW DATABASES`, schema enumeration, etc).
//!  - NONE:   no tables, not surfaced. Pure routing target (used by `CONNECT '<uri>'`).
//!  - AUTO:   backend's default. For all current backends this is equivalent to ENABLE.
//!  - HIDDEN: provides tables (asserted, same as ENABLE) but not surfaced in enumeration.
//!  - ENABLE: provides tables (asserted); surfaced normally. Errors if backend can't.
enum class CatalogMode : uint8_t { NONE, AUTO, HIDDEN, ENABLE };

//! Controls whether the attached database accepts `CONNECT`.
//!  - NONE:   explicitly disabled — `CONNECT` to this catalog errors, even if backend supports it.
//!  - AUTO:   backend's default — checked via `Supports(RemoteCapability::CONNECT)`.
//!  - ENABLE: explicitly required — `CONNECT` is allowed; if backend doesn't support it, the
//!            ATTACH itself errors so the user finds out at attach time rather than at first use.
enum class ConnectMode : uint8_t { NONE, AUTO, ENABLE };

//! DEFAULT is the standard ACID crash recovery mode.
//! NO_WAL_WRITES disables the WAL for the attached database, i.e., disabling the D in ACID.
//! Use this mode with caution, as it disables recovery from crashes for the file.
enum class RecoveryMode : uint8_t { DEFAULT = 0, NO_WAL_WRITES = 1 };

//! CHECKPOINT: Throws if the checkpoint fails.
//! TRY_CHECKPOINT: Does not throw when failing a checkpoint.
//! SKIP_CHECKPOINT: Skips checkpointing entirely.
//! All actions always clean up.
enum class DatabaseCloseAction { CHECKPOINT, TRY_CHECKPOINT, SKIP_CHECKPOINT };

class DatabaseFilePathManager;

struct StoredDatabasePath {
	StoredDatabasePath(DatabaseManager &db_manager, DatabaseFilePathManager &manager, string path, const string &name);
	~StoredDatabasePath();

	DatabaseManager &db_manager;
	DatabaseFilePathManager &manager;
	string path;

	void OnDetach();
};

//! AttachOptions holds information about a database we plan to attach. These options are generalized, i.e.,
//! they have to apply to any database file type (duckdb, sqlite, etc.).
struct AttachOptions {
	//! Constructor for databases we attach outside of the ATTACH DATABASE statement.
	explicit AttachOptions(const DBConfigOptions &options);
	//! Constructor for databases we attach when using ATTACH DATABASE.
	AttachOptions(const unordered_map<string, Value> &options, const AccessMode default_access_mode);

	//! Defaults to the access mode configured in the DBConfig, unless specified otherwise.
	AccessMode access_mode;
	//! The recovery type of the database.
	RecoveryMode recovery_mode = RecoveryMode::DEFAULT;
	//! The file format type. The default type is a duckdb database file, but other file formats are possible.
	string db_type;
	//! Set of remaining (key, value) options
	unordered_map<string, Value> options;
	//! (optionally) a catalog can be provided with a default table
	QualifiedName default_table;
	//! Whether this is the main database.
	bool is_main_database = false;
	//! Whether the attached database provides tables, and whether it surfaces in catalog
	//! enumeration. Defaults to AUTO (backend's choice; today equivalent to ENABLE for all
	//! current backends).
	CatalogMode catalog_mode = CatalogMode::AUTO;
	//! Whether `CONNECT` to this attached database is allowed. Defaults to AUTO (the
	//! backend's `Supports(RemoteCapability::CONNECT)` declaration decides).
	ConnectMode connect_mode = ConnectMode::AUTO;
	//! The stored database path (in the path manager)
	unique_ptr<StoredDatabasePath> stored_database_path;
	//! Per-database override of vacuum_rebuild_indexes. If not set, the global setting value is used.
	optional_idx vacuum_rebuild_indexes_threshold;
};

//! The AttachedDatabase represents an attached database instance.
class AttachedDatabase : public CatalogEntry, public enable_shared_from_this<AttachedDatabase> {
public:
	//! Create the built-in system database (without storage).
	explicit AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type = AttachedDatabaseType::SYSTEM_DATABASE);
	//! Create an attached database instance with the specified name and storage.
	AttachedDatabase(DatabaseInstance &db, Catalog &catalog, string name, string file_path, AttachOptions &options);
	//! Create an attached database instance with the specified storage extension.
	AttachedDatabase(DatabaseInstance &db, Catalog &catalog, StorageExtension &ext, ClientContext &context, string name,
	                 AttachInfo &info, AttachOptions &options);
	~AttachedDatabase() override;

	//! Initializes the catalog and storage of the attached database.
	void Initialize(optional_ptr<ClientContext> context = nullptr);
	void FinalizeLoad(optional_ptr<ClientContext> context);
	//! Close the database before shutting it down.
	void Close(const DatabaseCloseAction action);

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const override;
	bool HasStorageManager() const;
	StorageManager &GetStorageManager();
	const StorageManager &GetStorageManager() const;
	Catalog &GetCatalog();
	TransactionManager &GetTransactionManager();
	DatabaseInstance &GetDatabase() {
		return db;
	}

	optional_ptr<StorageExtension> GetStorageExtension() {
		return storage_extension;
	}

	const string &GetName() const {
		return name;
	}
	void SetName(const string &new_name) {
		name = new_name;
	}
	bool IsSystem() const;
	bool IsTemporary() const;
	bool IsReadOnly() const;
	bool IsInitialDatabase() const;
	void SetInitialDatabase();
	void SetReadOnlyDatabase();
	void OnDetach(ClientContext &context);
	RecoveryMode GetRecoveryMode() const {
		return recovery_mode;
	}
	//! vacuum_rebuild_indexes threshold for this attached database.
	//! Falls back to the global VacuumRebuildIndexesSetting if not overridden.
	idx_t GetVacuumRebuildIndexThreshold() const;
	CatalogMode GetCatalogMode() const {
		return catalog_mode;
	}
	//! True when the database does not appear in catalog enumeration (HIDDEN, or NONE which has
	//! no tables to expose anyway).
	bool IsHidden() const {
		return catalog_mode == CatalogMode::HIDDEN || catalog_mode == CatalogMode::NONE;
	}
	ConnectMode GetConnectMode() const {
		return connect_mode;
	}
	const unordered_map<string, Value> &GetAttachOptions() const {
		return attach_options;
	}
	string StoredPath() const;
	static bool NameIsReserved(const string &name);
	static string ExtractDatabaseName(const string &dbpath, FileSystem &fs);
	// Invoke Close() on an attached database, if its use count is 1.
	// Only call this in places where you know that the (last) shared pointer is about to go out of scope.
	static void InvokeCloseIfLastReference(shared_ptr<AttachedDatabase> &attached_database, ClientContext &context);

private:
	DatabaseInstance &db;
	unique_ptr<StoredDatabasePath> stored_database_path;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	AttachedDatabaseType type;
	optional_ptr<Catalog> parent_catalog;
	optional_ptr<StorageExtension> storage_extension;
	RecoveryMode recovery_mode = RecoveryMode::DEFAULT;
	CatalogMode catalog_mode = CatalogMode::AUTO;
	ConnectMode connect_mode = ConnectMode::AUTO;
	bool is_initial_database = false;
	bool is_closed = false;
	shared_ptr<mutex> close_lock;
	optional_idx vacuum_rebuild_threshold;
	unordered_map<string, Value> attach_options;

private:
	//! Clean any (shared) resources held by the database.
	void Cleanup();
};

} // namespace duckdb
