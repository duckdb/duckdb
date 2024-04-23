//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
class BufferManager;
class DatabaseManager;
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;
class TaskScheduler;
class ObjectCache;
struct AttachInfo;
class DatabaseFileSystem;

struct ExtensionInfo {
	explicit ExtensionInfo(const string &version) : extension_version(version) {
	}
	ExtensionInfo() : ExtensionInfo("defaultme") {
	}
	ExtensionInfo(const ExtensionInfo &x) : ExtensionInfo(x.extension_version) {
	}
	string extension_version;
};

class DatabaseInstance : public enable_shared_from_this<DatabaseInstance> {
	friend class DuckDB;

public:
	DUCKDB_API DatabaseInstance();
	DUCKDB_API ~DatabaseInstance();

	DBConfig config;

public:
	BufferPool &GetBufferPool() const;
	DUCKDB_API SecretManager &GetSecretManager();
	DUCKDB_API BufferManager &GetBufferManager();
	DUCKDB_API const BufferManager &GetBufferManager() const;
	DUCKDB_API DatabaseManager &GetDatabaseManager();
	DUCKDB_API FileSystem &GetFileSystem();
	DUCKDB_API TaskScheduler &GetScheduler();
	DUCKDB_API ObjectCache &GetObjectCache();
	DUCKDB_API ConnectionManager &GetConnectionManager();
	DUCKDB_API ValidChecker &GetValidChecker();
	DUCKDB_API void SetExtensionLoaded(const std::string &extension_name, const std::string &extension_version = "");

	idx_t NumberOfThreads();

	DUCKDB_API static DatabaseInstance &GetDatabase(ClientContext &context);
	DUCKDB_API static const DatabaseInstance &GetDatabase(const ClientContext &context);

	DUCKDB_API const unordered_set<std::string> &LoadedExtensions();
	DUCKDB_API const unordered_map<string, ExtensionInfo> &LoadedExtensionsData();
	DUCKDB_API bool ExtensionIsLoaded(const string &name);

	DUCKDB_API SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) const;

	unique_ptr<AttachedDatabase> CreateAttachedDatabase(ClientContext &context, const AttachInfo &info,
	                                                    const string &type, AccessMode access_mode);

private:
	void Initialize(const char *path, DBConfig *config);
	void CreateMainDatabase();

	void Configure(DBConfig &config, const char *path);

private:
	shared_ptr<BufferManager> buffer_manager;
	unique_ptr<DatabaseManager> db_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ObjectCache> object_cache;
	unique_ptr<ConnectionManager> connection_manager;
	unordered_set<string> loaded_extensions;
	unordered_map<string, ExtensionInfo> loaded_extensions_data;
	ValidChecker db_validity;
	unique_ptr<DatabaseFileSystem> db_file_system;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
public:
	DUCKDB_API explicit DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(const string &path, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(DatabaseInstance &instance);

	DUCKDB_API ~DuckDB();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	template <class T>
	void LoadExtension() {
		T extension;
		if (ExtensionIsLoaded(extension.Name())) {
			return;
		}
		extension.Load(*this);
		instance->SetExtensionLoaded(extension.Name());
	}

	DUCKDB_API FileSystem &GetFileSystem();

	DUCKDB_API idx_t NumberOfThreads();
	DUCKDB_API static const char *SourceID();
	DUCKDB_API static const char *LibraryVersion();
	DUCKDB_API static idx_t StandardVectorSize();
	DUCKDB_API static string Platform();
	DUCKDB_API bool ExtensionIsLoaded(const std::string &name);
};

} // namespace duckdb
