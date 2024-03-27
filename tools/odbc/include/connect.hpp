#ifndef DUCKDB_CONNECT_H
#define DUCKDB_CONNECT_H

#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"

#include <locale>
#include <odbcinst.h>

#define ROW_DEL     ';'
#define KEY_VAL_DEL '='

namespace duckdb {

class Connect {
public:
	Connect(OdbcHandleDbc *dbc_p, std::string input_str_p);
	~Connect() = default;

	bool SetSuccessWithInfo(SQLRETURN ret);
	SQLRETURN ParseInputStr();
	// Splits the row into a key and value, and using SetVal sets the variable for the key
	SQLRETURN FindKeyValPair(const std::string &row);
	// Find the key that matches the keyname
	SQLRETURN FindMatchingKey(const std::string &input, string &key);
	SQLRETURN ReadFromIniFile();
	// Executes the connection
	SQLRETURN SetConnection();

	// getters
	std::string GetInputStr() {
		return input_str;
	}
	bool GetSuccessWithInfo() const {
		return success_with_info;
	}
	// Ignore keys for use with Power Query
	std::vector<std::string> PQIgnoreKeys = {"driver"};

private:
	OdbcHandleDbc *dbc;
	std::string input_str;
	bool success_with_info = false;

	map<string, bool> seen_config_options;
	case_insensitive_map_t<Value> config_map;
	duckdb::DBConfig config;
};

} // namespace duckdb
#endif // DUCKDB_CONNECT_H
