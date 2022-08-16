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

namespace duckdb {
class DBInstanceCache {
public:
	DBInstanceCache() {};
	//! Gets a DB Instance from the cache if already exists (Fails if the configurations do not match)
	shared_ptr<DuckDB> GetInstance(const string &abs_database_path, const unordered_map<string, string> &config_dict,
	                               bool read_only);

	//! Creates and caches a new DB Instance (Fails if a cached instance already exists)
	shared_ptr<DuckDB> CreateInstance(const string &abs_database_path,
	                                  const std::unordered_map<string, string> &config_dict, bool read_only,
	                                  bool cache_instance = true);

private:
	//! A map with the cached instances <absolute_path/instance>
	unordered_map<string, weak_ptr<DuckDB>> db_instances;

	//! Lock to alter cache
	mutex cache_lock;

	//! Checks if a DBConfig matches with a config dictionary
	bool IsConfigurationSame(ClientContext *context, DBConfig &config,
	                         const std::unordered_map<string, string> &config_dict, bool read_only);

};
} // namespace duckdb
