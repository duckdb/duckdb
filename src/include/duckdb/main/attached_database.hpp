//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/attached_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
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

enum class AttachVisibility { SHOWN, HIDDEN };

//! DEFAULT is the standard ACID crash recovery mode.
//! NO_WAL_WRITES disables the WAL for the attached database, i.e., disabling the D in ACID.
//! Use this mode with caution, as it disables recovery from crashes for the file.
enum class RecoveryMode : uint8_t { DEFAULT = 0, NO_WAL_WRITES = 1 };

class DatabaseFilePathManager;

struct StoredDatabasePath {
	StoredDatabasePath(DatabaseManager &db_manager, DatabaseFilePathManager &manager, string path, const string &name);
	~StoredDatabasePath();

	DatabaseManager &db_manager;
	DatabaseFilePathManager &manager;
	string path;

public:
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
	//! Whether or not this is the main database
	bool is_main_database = false;
	//! The visibility of the attached database
	AttachVisibility visibility = AttachVisibility::SHOWN;
	//! The stored database path (in the path manager)
	unique_ptr<StoredDatabasePath> stored_database_path;
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
	void Close();

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const override;
	bool HasStorageManager() const;
	StorageManager &GetStorageManager();
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
	AttachVisibility GetVisibility() const {
		return visibility;
	}
	string StoredPath() const;

	static bool NameIsReserved(const string &name);
	static string ExtractDatabaseName(const string &dbpath, FileSystem &fs);

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
	AttachVisibility visibility = AttachVisibility::SHOWN;
	bool is_initial_database = false;
	bool is_closed = false;
};

} // namespace duckdb
