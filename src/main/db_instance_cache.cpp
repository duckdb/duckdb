#include "duckdb/main/db_instance_cache.hpp"

namespace duckdb {
bool DBInstanceCache::IsConfigurationSame(ClientContext *context, DBConfig &config,
                                          const std::unordered_map<string, string> &config_dict, bool read_only) {
	if (read_only) {
		if (config.options.access_mode != AccessMode::READ_ONLY) {
			return false;
		}
	} else {
		if (config.options.access_mode == AccessMode::READ_ONLY) {
			return false;
		}
	}
	for (auto &kv : config_dict) {
		string key = kv.first;
		auto val = Value(kv.second);
		auto cur_val = config.GetOptionByName(key)->get_setting(*context);
		if (cur_val != val) {
			return false;
		}
	}
	return true;
}

shared_ptr<DuckDB> DBInstanceCache::GetInstance(const string &abs_database_path,
                                                const std::unordered_map<string, string> &config_dict, bool read_only) {
	shared_ptr<DuckDB> db_instance;
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		db_instance = db_instances[abs_database_path].lock();
		if (db_instance) {
			auto existing_context = db_instance->instance->GetConnectionManager().GetConnectionList().front().get();
			if (!IsConfigurationSame(existing_context, db_instance->instance->config, config_dict, read_only)) {
				throw std::runtime_error("Can't open a connection to same database file with a different configuration "
				                         "than existing connections");
			}
		}

		db_instance = db_instances[abs_database_path].lock();
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstance(const string &abs_database_path,
                                                   const std::unordered_map<string, string> &config_dict,
                                                   bool read_only, bool cache_instance) {
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		throw std::runtime_error("Instance with path: " + abs_database_path + " already exists.");
	}
	DBConfig config;
	if (read_only) {
		config.options.access_mode = AccessMode::READ_ONLY;
	}
	for (auto &kv : config_dict) {
		string key = kv.first;
		string val = kv.second;
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			throw InvalidInputException("Unrecognized configuration property \"%s\"", key);
		}
		config.SetOption(*config_property, Value(val));
	}
	// Creates new instance
	auto db_instance = make_shared<DuckDB>(abs_database_path, &config);
	if (cache_instance) {
		db_instances[abs_database_path] = db_instance;
	}
	return db_instance;
}

} // namespace duckdb
