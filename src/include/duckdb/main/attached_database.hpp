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
#include "duckdb/storage/storage_options.hpp"

namespace duckdb {
class Catalog;
class DatabaseInstance;
class StorageManager;
class TransactionManager;
class StorageExtension;
class DatabaseManager;

struct AttachInfo;

enum class AttachedDatabaseType {
	READ_WRITE_DATABASE,
	READ_ONLY_DATABASE,
	SYSTEM_DATABASE,
	TEMP_DATABASE,
};

//! AttachOptions holds information about a database we plan to attach. These options are generalized, i.e.,
//! they have to apply to any database file type (duckdb, sqlite, etc.).
struct AttachOptions {
	//! Constructor for databases we attach outside of the ATTACH DATABASE statement.
	explicit AttachOptions(const DBConfigOptions &options);
	//! Constructor for databases we attach when using ATTACH DATABASE.
	AttachOptions(const unique_ptr<AttachInfo> &info, const AccessMode default_access_mode);

	//! Defaults to the access mode configured in the DBConfig, unless specified otherwise.
	AccessMode access_mode;
	//! The file format type. The default type is a duckdb database file, but other file formats are possible.
	string db_type;
	//! Set of remaining (key, value) options
	unordered_map<string, Value> options;
	//! (optionally) a catalog can be provided with a default table
	QualifiedName default_table;
};

//! The AttachedDatabase represents an attached database instance.
class AttachedDatabase : public CatalogEntry {
public:
	//! Create the built-in system database (without storage).
	explicit AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type = AttachedDatabaseType::SYSTEM_DATABASE);
	//! Create an attached database instance with the specified name and storage.
	AttachedDatabase(DatabaseInstance &db, Catalog &catalog, string name, string file_path,
	                 const AttachOptions &options);
	//! Create an attached database instance with the specified storage extension.
	AttachedDatabase(DatabaseInstance &db, Catalog &catalog, StorageExtension &ext, ClientContext &context, string name,
	                 const AttachInfo &info, const AttachOptions &options);
	~AttachedDatabase() override;

	//! Initializes the catalog and storage of the attached database.
	void Initialize(optional_ptr<ClientContext> context = nullptr, StorageOptions options = StorageOptions());
	void Close();

	Catalog &ParentCatalog() override;
	const Catalog &ParentCatalog() const override;
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
	bool IsSystem() const;
	bool IsTemporary() const;
	bool IsReadOnly() const;
	bool IsInitialDatabase() const;
	void SetInitialDatabase();
	void SetReadOnlyDatabase();

	static bool NameIsReserved(const string &name);
	static string ExtractDatabaseName(const string &dbpath, FileSystem &fs);

private:
	DatabaseInstance &db;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	AttachedDatabaseType type;
	optional_ptr<Catalog> parent_catalog;
	optional_ptr<StorageExtension> storage_extension;
	bool is_initial_database = false;
	bool is_closed = false;
};

} // namespace duckdb
