#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

string GetDBAbsolutePath(const string &database_p, FileSystem &fs) {
	auto database = FileSystem::ExpandPath(database_p, nullptr);
	if (database.empty()) {
		return ":memory:";
	}
	if (database.rfind(":memory:", 0) == 0) {
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
	shared_ptr<DuckDB> db_instance;

	auto local_fs = FileSystem::CreateLocal();
	auto abs_database_path = GetDBAbsolutePath(database, *local_fs);
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		db_instance = db_instances[abs_database_path].lock();
		if (db_instance) {
			if (db_instance->instance->config != config) {
				throw duckdb::ConnectionException(
				    "Can't open a connection to same database file with a different configuration "
				    "than existing connections");
			}
		} else {
			// clean-up
			db_instances.erase(abs_database_path);
		}
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::GetInstance(const string &database, const DBConfig &config) {
	lock_guard<mutex> l(cache_lock);
	return GetInstanceInternal(database, config);
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstanceInternal(const string &database, DBConfig &config,
                                                           bool cache_instance) {
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
	if (abs_database_path.rfind(":memory:", 0) == 0) {
		instance_path = ":memory:";
	}
	auto db_instance = make_shared<DuckDB>(instance_path, &config);
	if (cache_instance) {
		db_instances[abs_database_path] = db_instance;
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstance(const string &database, DBConfig &config, bool cache_instance) {
	lock_guard<mutex> l(cache_lock);
	return CreateInstanceInternal(database, config, cache_instance);
}

shared_ptr<DuckDB> DBInstanceCache::GetOrCreateInstance(const string &database, DBConfig &config_dict,
                                                        bool cache_instance) {
	lock_guard<mutex> l(cache_lock);
	if (cache_instance) {
		auto instance = GetInstanceInternal(database, config_dict);
		if (instance) {
			return instance;
		}
	}
	return CreateInstanceInternal(database, config_dict, cache_instance);
}

} // namespace duckdb
