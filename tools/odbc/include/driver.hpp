#ifndef DRIVER_HPP
#define DRIVER_HPP

#include "duckdb_odbc.hpp"

#define ROW_DEL ';'
#define KEY_VAL_DEL '='

namespace duckdb {

class Connect {
public:
	enum ODBCConnStrKey {
		DATABASE,
		DSN,
	};

	const std::map<ODBCConnStrKey, std::string> conn_str_keynames = {
	    {DATABASE, "database"},
	    {DSN, "dsn"},
	};

	std::map<ODBCConnStrKey, bool> set_keys = {
	    {DATABASE, false},
	    {DSN, false},
	};

	using f = SQLRETURN (Connect::*)(const std::string &);
	const std::map<ODBCConnStrKey, f> handle_functions = {
	    {DATABASE, &Connect::HandleDatabase},
	    {DSN, &Connect::HandleDsn},
	};

public:
	Connect(OdbcHandleDbc *dbc_p, const std::string &input_str_p) : dbc(dbc_p), input_str(input_str_p) {};
	~Connect() {};

	// Checks if the key is set
	bool CheckSet(ODBCConnStrKey key) { return set_keys.at(key); }
	// Sets the variable for the key
	SQLRETURN SetVal(ODBCConnStrKey key, const std::string &val);
	// Checks if success with info has been returned and sets the variable
	bool SetSuccessWithInfo(SQLRETURN ret);
	SQLRETURN ParseInputStr();
	// Splits the row into a key and value, and using SetVal sets the variable for the key
	SQLRETURN FindKeyValPair(const std::string &row);
	// Find the key that matches the keyname
	SQLRETURN FindMatchingKey(const std::string &input, ODBCConnStrKey &key);
	// If the input doesn't match a keyname, find a similar keyname
	bool FindSimilar(const std::string &input, std::string &match);
	SQLRETURN ReadFromIniFile();
	// Executes the connection
	SQLRETURN SetConnection();

	// getters
	std::string GetInputStr() { return input_str; }
	bool GetSuccessWithInfo() { return success_with_info; }

	// handle values
	SQLRETURN HandleDatabase(const std::string &val);
	SQLRETURN HandleDsn(const std::string &val);

private:
	OdbcHandleDbc *dbc;
	std::string input_str;
	bool success_with_info = false;
	duckdb::DBConfig config;
};

SQLRETURN FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle);

} // namespace duckdb

#endif
