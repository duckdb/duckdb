#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

DatabaseCacheEntry::DatabaseCacheEntry() {
}

DatabaseCacheEntry::DatabaseCacheEntry(const shared_ptr<DuckDB> &database_p) : database(database_p) {
}

DatabaseCacheEntry::~DatabaseCacheEntry() {
}

string GetDBAbsolutePath(const string &database_p, FileSystem &fs) {
	auto database = FileSystem::ExpandPath(database_p, nullptr);
	if (database.empty()) {
		return IN_MEMORY_PATH;
	}
	if (database.rfind(IN_MEMORY_PATH, 0) == 0) {
		// this is a memory db, just return it.
		return database;
	}
	if (!ExtensionHelper::ExtractExtensionPrefixFromPath(database).empty()) {
		// this database path is handled by a replacement open and is not a file path
		return database;
	}
	if (fs.IsPathAbsolute(database)) {
		return fs.NormalizeAbsolutePath(database);
	}
	return fs.NormalizeAbsolutePath(fs.JoinPath(FileSystem::GetWorkingDirectory(), database));
}

shared_ptr<DuckDB> DBInstanceCache::GetInstanceInternal(const string &database, const DBConfig &config) {
	auto local_fs = FileSystem::CreateLocal();
	auto abs_database_path = GetDBAbsolutePath(database, *local_fs);
	auto entry = db_instances.find(abs_database_path);
	if (entry == db_instances.end()) {
		// path does not exist in the list yet - no cache entry
		return nullptr;
	}
	auto cache_entry = entry->second.lock();
	if (!cache_entry) {
		// cache entry does not exist anymore - clean it up
		db_instances.erase(entry);
		return nullptr;
	}
	// cache entry exists - check if the actual database still exists
	auto db_instance = cache_entry->database.lock();
	if (!db_instance) {
		// if the database does not exist, but the cache entry still exists, the database is being shut down
		// we need to wait until the database is fully shut down to safely proceed
		// we do this here using a busy spin
		while (cache_entry) {
			// clear our cache entry
			cache_entry.reset();
			// try to lock it again
			cache_entry = entry->second.lock();
		}
		// the cache entry has now been deleted - clear it from the set of database instances and return
		db_instances.erase(entry);
		return nullptr;
	}
	// the database instance exists - check that the config matches
	if (db_instance->instance->config != config) {
		throw duckdb::ConnectionException(
		    "Can't open a connection to same database file with a different configuration "
		    "than existing connections");
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::GetInstance(const string &database, const DBConfig &config) {
	lock_guard<mutex> l(cache_lock);
	return GetInstanceInternal(database, config);
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstanceInternal(const string &database, DBConfig &config,
                                                           bool cache_instance,
                                                           const std::function<void(DuckDB &)> &on_create) {
	string abs_database_path;
	if (config.file_system) {
		abs_database_path = GetDBAbsolutePath(database, *config.file_system);
	} else {
		auto tmp_fs = FileSystem::CreateLocal();
		abs_database_path = GetDBAbsolutePath(database, *tmp_fs);
	}
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		throw duckdb::Exception(ExceptionType::CONNECTION,
		                        "Instance with path: " + abs_database_path + " already exists.");
	}
	// Creates new instance
	string instance_path = abs_database_path;
	if (abs_database_path.rfind(IN_MEMORY_PATH, 0) == 0) {
		instance_path = IN_MEMORY_PATH;
	}
	shared_ptr<DatabaseCacheEntry> cache_entry;
	if (cache_instance) {
		cache_entry = make_shared_ptr<DatabaseCacheEntry>();
		config.db_cache_entry = cache_entry;
	}
	auto db_instance = make_shared_ptr<DuckDB>(instance_path, &config);
	if (cache_entry) {
		// attach cache entry to the database
		cache_entry->database = db_instance;

		// cache the entry in the db_instances map
		db_instances[abs_database_path] = cache_entry;
	}
	if (on_create) {
		on_create(*db_instance);
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstance(const string &database, DBConfig &config, bool cache_instance,
                                                   const std::function<void(DuckDB &)> &on_create) {
	lock_guard<mutex> l(cache_lock);
	return CreateInstanceInternal(database, config, cache_instance, on_create);
}

shared_ptr<DuckDB> DBInstanceCache::GetOrCreateInstance(const string &database, DBConfig &config_dict,
                                                        bool cache_instance,
                                                        const std::function<void(DuckDB &)> &on_create) {
	lock_guard<mutex> l(cache_lock);
	if (cache_instance) {
		auto instance = GetInstanceInternal(database, config_dict);
		if (instance) {
			return instance;
		}
	}
	return CreateInstanceInternal(database, config_dict, cache_instance, on_create);
}

} // namespace duckdb
