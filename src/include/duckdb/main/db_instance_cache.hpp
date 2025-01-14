//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/db_instance_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include <functional>

namespace duckdb {
class DBInstanceCache;

struct DatabaseCacheEntry {
	DatabaseCacheEntry();
	explicit DatabaseCacheEntry(const shared_ptr<DuckDB> &database);
	~DatabaseCacheEntry();

	weak_ptr<DuckDB> database;
};

class DBInstanceCache {
public:
	DBInstanceCache() {
	}

	//! Gets a DB Instance from the cache if already exists (Fails if the configurations do not match)
	shared_ptr<DuckDB> GetInstance(const string &database, const DBConfig &config_dict);

	//! Creates and caches a new DB Instance (Fails if a cached instance already exists)
	shared_ptr<DuckDB> CreateInstance(const string &database, DBConfig &config_dict, bool cache_instance = true,
	                                  const std::function<void(DuckDB &)> &on_create = nullptr);

	//! Either returns an existing entry, or creates and caches a new DB Instance
	shared_ptr<DuckDB> GetOrCreateInstance(const string &database, DBConfig &config_dict, bool cache_instance,
	                                       const std::function<void(DuckDB &)> &on_create = nullptr);

private:
	//! A map with the cached instances <absolute_path/instance>
	unordered_map<string, weak_ptr<DatabaseCacheEntry>> db_instances;

	//! Lock to alter cache
	mutex cache_lock;

private:
	shared_ptr<DuckDB> GetInstanceInternal(const string &database, const DBConfig &config_dict);
	shared_ptr<DuckDB> CreateInstanceInternal(const string &database, DBConfig &config_dict, bool cache_instance,
	                                          const std::function<void(DuckDB &)> &on_create);
};
} // namespace duckdb
