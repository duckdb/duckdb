#ifndef DRIVER_HPP
#define DRIVER_HPP

#include "duckdb_odbc.hpp"

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
	    {DSN, &Connect::HandleDsn},
	};

public:
	Connect(OdbcHandleDbc *dbc_p, const std::string &input_str_p) : dbc(dbc_p), input_str(input_str_p) {};
	~Connect() {};

	// getters
	std::string GetInputStr() { return input_str; }
	bool GetSuccessWithInfo() { return success_with_info; }

	SQLRETURN SetConnection();

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
