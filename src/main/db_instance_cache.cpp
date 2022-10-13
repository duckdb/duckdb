#include "duckdb/main/db_instance_cache.hpp"

namespace duckdb {

string GetDBAbsolutePath(const string &database) {
	if (database.empty()) {
		return ":memory:";
	}
	if (database.rfind(":memory:", 0) == 0) {
		// this is a memory db, just return it.
		return database;
	}
	if (FileSystem::IsPathAbsolute(database)) {
		return database;
	}
	return FileSystem::JoinPath(FileSystem::GetWorkingDirectory(), database);
}

shared_ptr<DuckDB> DBInstanceCache::GetInstance(const string &database, const DBConfig &config) {
	lock_guard<mutex> l(cache_lock);
	shared_ptr<DuckDB> db_instance;
	auto abs_database_path = GetDBAbsolutePath(database);
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		db_instance = db_instances[abs_database_path].lock();
		if (db_instance) {
			if (db_instance->instance->config != config) {
				throw duckdb::Exception(ExceptionType::CONNECTION,
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

shared_ptr<DuckDB> DBInstanceCache::CreateInstance(const string &database, DBConfig &config, bool cache_instance) {
	lock_guard<mutex> l(cache_lock);
	auto abs_database_path = GetDBAbsolutePath(database);
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

} // namespace duckdb
