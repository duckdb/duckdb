#include "../common.h"
#include <fstream>

using namespace odbc_test;

// Connect to database using SQLDriverConnect without a DSN
void ConnectWithoutDSN(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string conn_str = "";
	SQLCHAR str[1024];
	SQLSMALLINT strl;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	EXECUTE_AND_CHECK("SQLDriverConnect", SQLDriverConnect, dbc, nullptr, ConvertToSQLCHAR(conn_str.c_str()), SQL_NTS,
	                  str, sizeof(str), &strl, SQL_DRIVER_COMPLETE);
}

// Connect with incorrect params
void ConnectWithIncorrectParam(std::string param, std::vector<std::string> expected_msg) {
	SQLHANDLE env;
	SQLHANDLE dbc;
	std::string dsn = "DSN=duckdbmemory;" + param;
	SQLCHAR str[1024];
	SQLSMALLINT strl;

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR(dsn.c_str()), SQL_NTS, str, sizeof(str), &strl,
	                       SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, dbc, SQL_HANDLE_DBC);
	for (const auto &msg : expected_msg) {
		REQUIRE(duckdb::StringUtil::Contains(message, msg));
	}

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test sending incorrect parameters to SQLDriverConnect
static void TestIncorrectParams() {
	ConnectWithIncorrectParam("UnsignedAttribute=true", {"Invalid keyword", "allow_unsigned_extensions"});
	ConnectWithIncorrectParam("dtabase=test.duckdb", {"Invalid keyword", "database"});
	ConnectWithIncorrectParam("this_doesnt_exist=?", {"Invalid keyword"});
}

// Check if database is correctly set
static void CheckDatabase(SQLHANDLE &dbc) {
	HSTMT hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Select * from customers
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT * FROM customer)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM customer"), SQL_NTS);

	// Fetch the first row
	idx_t i = 1;
	while (SQLFetch(hstmt) == SQL_SUCCESS) {
		// Fetch the next row
		DATA_CHECK(hstmt, 1, std::to_string(i++));
	}
	REQUIRE(i == 15001);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

// Test setting a database from the connection string
static void TestSettingDatabase() {
	SQLHANDLE env;
	SQLHANDLE dbc;

	auto db_path = "Database=" + GetTesterDirectory() + "test.duckdb";

	// Connect to database using a connection string with a database path
	DRIVER_CONNECT_TO_DATABASE(env, dbc, db_path);

	// Check that the connection was successful
	CheckDatabase(dbc);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Checks if config is correctly set
static void CheckConfig(SQLHANDLE &dbc, const std::string &setting, const std::string &expected_content) {
	HSTMT hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check if the setting is successfully changed
	EXECUTE_AND_CHECK("SQLExecDirect (select current_setting('" + setting + "'))", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("select current_setting('" + setting + "')"), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, duckdb::StringUtil::Lower(expected_content));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

// Connect with connection string that sets a specific config then checks if correctly set
static void SetConfig(const std::string &param, const std::string &setting, const std::string &expected_content) {
	SQLHANDLE env;
	SQLHANDLE dbc;

	DRIVER_CONNECT_TO_DATABASE(env, dbc, param);

	CheckConfig(dbc, setting, expected_content);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test setting different configs through the connection string
static void TestSettingConfigs() {
	// Test setting allow_unsigned_extensions
	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "allow_unsigned_extensions=true",
	          "allow_unsigned_extensions", "true");

	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "allow_unsigned_extensions=false",
	          "allow_unsigned_extensions", "false");

	SetConfig("allow_unsigned_extensions=true", "allow_unsigned_extensions", "true");

	SetConfig("allow_unsigned_extensions=false", "allow_unsigned_extensions", "false");

	// Test setting access_mode
	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "access_mode=READ_ONLY", "access_mode",
	          "READ_ONLY");

	SetConfig("Database=" + GetTesterDirectory() + "test.duckdb;" + "access_mode=READ_WRITE", "access_mode",
	          "READ_WRITE");
}

// Test input from an ini file
static void TestIniFile() {
	// Create a temporary ini file
	std::string ini_file = GetHomeDirectory() + "/.odbc.ini";
	std::ofstream out(ini_file);
	out << "[DuckDB]\n";
	out << "Driver = DuckDB Driver\n";
	out << "database = " + GetTesterDirectory() + "test.duckdb\n";
//	out << "access_mode = read_only\n";
//	out << "allow_unsigned_extensions = true\n";
	out.close();

	// Connect to the database using the ini file
	SQLHANDLE env;
	SQLHANDLE dbc;
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "DSN=DuckDB");

	// Check that the database is set
	CheckDatabase(dbc);

//	// Check that database is read only
//	CheckConfig(dbc, "access_mode", "read_only");
//
//	// Check that allow_unsigned_extensions is set
//	CheckConfig(dbc, "allow_unsigned_extensions", "true");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Delete the ini file
	std::remove(ini_file.c_str());
}

TEST_CASE("Test SQLConnect and SQLDriverConnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
	DISCONNECT_FROM_DATABASE(env, dbc);

//	TestIncorrectParams();

	TestSettingDatabase();

//	TestSettingConfigs();

	ConnectWithoutDSN(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	TestIniFile();
}

TEST_CASE("Test user_agent - in-memory database", "[odbc][useragent]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc') FROM pragma_user_agent()"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test user_agent - named database", "[odbc][useragent]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc') FROM pragma_user_agent()"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// In-memory databases are a singleton from duckdb_odbc.hpp, so cannot have custom options
TEST_CASE("Test user_agent - named database, custom useragent", "[odbc][useragent]") {
//	SQLHANDLE env;
//	SQLHANDLE dbc;
//
//	HSTMT hstmt = SQL_NULL_HSTMT;
//
//	// Connect to the database using SQLConnect with a custom user_agent
//	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named_ua.db;custom_user_agent=CUSTOM_STRING");
//
//	// Allocate a statement handle
//	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
//
//	// Execute a simple query
//	EXECUTE_AND_CHECK(
//	    "SQLExecDirect (get user_agent)", SQLExecDirect, hstmt,
//	    ConvertToSQLCHAR(
//	        "SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc CUSTOM_STRING') FROM pragma_user_agent()"),
//	    SQL_NTS);
//
//	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", SQLFetch, hstmt);
//	DATA_CHECK(hstmt, 1, "true");
//
//	// Free the env handle
//	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
//
//	DISCONNECT_FROM_DATABASE(env, dbc);
}
