#include "connect.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include <iostream>
#include <utility>

#if WIN32
#include <windows.h>
#endif

using namespace duckdb;

//! The database instance cache, used so that multiple connections to the same file point to the same database object
DBInstanceCache instance_cache;

bool Connect::SetSuccessWithInfo(SQLRETURN ret) {
	if (!SQL_SUCCEEDED(ret)) {
		return false;
	}
	if (ret == SQL_SUCCESS_WITH_INFO) {
		success_with_info = true;
	}
	return true;
}

SQLRETURN Connect::FindMatchingKey(const std::string &input, string &key) {
	if (seen_config_options.find(input) != seen_config_options.end()) {
		key = input;
		return SQL_SUCCESS;
	}

	auto config_names = DBConfig::GetOptionNames();

	// If the input doesn't match a keyname, find a similar keyname
	auto msg = StringUtil::CandidatesErrorMessage(config_names, input, "Did you mean: ");
	return SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect",
	                           "Invalid keyword: '" + input + "'. " + msg, SQLStateType::ST_01S09, "");
}

SQLRETURN Connect::FindKeyValPair(const std::string &row) {
	string key;

	size_t val_pos = row.find(KEY_VAL_DEL);
	if (val_pos == std::string::npos) {
		// an equal '=' char must be present (syntax error)
		return (SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Invalid connection string",
		                            SQLStateType::ST_HY000, ""));
	}

	std::string key_candidate = StringUtil::Lower(row.substr(0, val_pos));

	// Check if the key can be ignored
	if (std::find(PQIgnoreKeys.begin(), PQIgnoreKeys.end(), key_candidate) != PQIgnoreKeys.end()) {
		return SQL_SUCCESS;
	}

	SQLRETURN ret = FindMatchingKey(key_candidate, key);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	config_map[key] = row.substr(val_pos + 1);
	seen_config_options[key] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::ParseInputStr() {
	size_t row_pos;
	std::string row;

	if (input_str.empty()) {
		return SQL_SUCCESS;
	}

	while ((row_pos = input_str.find(ROW_DEL)) != std::string::npos) {
		row = input_str.substr(0, row_pos);
		SQLRETURN ret = FindKeyValPair(row);
		if (ret != SQL_SUCCESS) {
			return ret;
		}
		input_str.erase(0, row_pos + 1);
	}

	if (input_str.empty()) {
		return SQL_SUCCESS;
	}

	SQLRETURN ret = FindKeyValPair(input_str);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	// Extract the DSN from the config map as it is needed to read from the .odbc.ini file
	dbc->dsn = seen_config_options["dsn"] ? config_map["dsn"].ToString() : "";

	return SQL_SUCCESS;
}

SQLRETURN Connect::ReadFromIniFile() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	if (dbc->dsn.empty()) {
		return SQL_SUCCESS;
	}

	auto converted_dsn = OdbcUtils::ConvertStringToLPCSTR(dbc->dsn);
	for (auto &key_pair : seen_config_options) {
		// if the key has already been set, skip it
		if (key_pair.second) {
			continue;
		}
		const idx_t max_val_len = 256;
		char char_val[max_val_len];
		auto converted_key = key_pair.first.c_str();
		int read_size = SQLGetPrivateProfileString(converted_dsn, converted_key, "", char_val, max_val_len, "odbc.ini");
		if (read_size == 0) {
			continue;
		} else if (read_size < 0) {
			return SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Error reading from .odbc.ini",
			                           SQLStateType::ST_01S09, "");
		}
		config_map[key_pair.first] = std::string(char_val);
		seen_config_options[key_pair.first] = true;
	}
#endif
	return SQL_SUCCESS;
}

SQLRETURN Connect::SetConnection() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	ReadFromIniFile();
#endif
	auto database = seen_config_options["database"] ? config_map["database"].ToString() : IN_MEMORY_PATH;
	dbc->SetDatabaseName(database);

	// remove the database and dsn from the config map since they aren't config options
	config_map.erase("database");
	config_map.erase("dsn");

	config.SetOptionByName("duckdb_api", "odbc");
	config.SetOptionsByName(config_map);

	bool cache_instance = database != IN_MEMORY_PATH;
	dbc->env->db = instance_cache.GetOrCreateInstance(database, config, cache_instance);

	if (!dbc->conn) {
		dbc->conn = make_uniq<Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

Connect::Connect(OdbcHandleDbc *dbc_p, string input_str_p) : dbc(dbc_p), input_str(std::move(input_str_p)) {
	// Get all the config options
	auto config_options = DBConfig::GetOptionNames();
	for (auto &option : config_options) {
		// They are all set to false as they haven't been set yet
		seen_config_options[option] = false;
	}
	seen_config_options["dsn"] = false;
	seen_config_options["database"] = false;
}
