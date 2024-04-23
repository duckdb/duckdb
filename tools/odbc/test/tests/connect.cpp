#include "connect_helpers.h"

#include <iostream>
#include <odbcinst.h>

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

// Connect to a database with extra keywords provided by Power Query SDK
void ConnectWithPowerQuerySDK(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string conn_str = "DRIVER={DuckDB Driver};database=" + GetTesterDirectory() +
	                       +";custom_user_agent=powerbi/v0.0(DuckDB);Trusted_Connection=yes;";
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
void ConnectWithIncorrectParam(std::string param) {
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
	REQUIRE(duckdb::StringUtil::Contains(message, "Invalid keyword"));
	REQUIRE(duckdb::StringUtil::Contains(message, "Did you mean: "));

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test sending incorrect parameters to SQLDriverConnect
static void TestIncorrectParams() {
	ConnectWithIncorrectParam("UnsignedAttribute=true");
	ConnectWithIncorrectParam("dtabase=test.duckdb");
	ConnectWithIncorrectParam("this_doesnt_exist=?");
}

// Test setting a database from the connection string
static void TestSettingDatabase() {
	SQLHANDLE env;
	SQLHANDLE dbc;

	auto db_path = "Database=" + GetTesterDirectory();

	// Connect to database using a connection string with a database path
	DRIVER_CONNECT_TO_DATABASE(env, dbc, db_path);

	// Check that the connection was successful
	CheckDatabase(dbc);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
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

TEST_CASE("Test SQLConnect and SQLDriverConnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
	DISCONNECT_FROM_DATABASE(env, dbc);

	TestIncorrectParams();

	TestSettingDatabase();

	TestSettingConfigs();

	ConnectWithoutDSN(env, dbc);
	ConnectWithPowerQuerySDK(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);
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
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect with a custom user_agent
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named_ua.db;custom_user_agent=CUSTOM_STRING");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK(
	    "SQLExecDirect (get user_agent)", SQLExecDirect, hstmt,
	    ConvertToSQLCHAR(
	        "SELECT regexp_matches(user_agent, '^duckdb/.*(.*) odbc CUSTOM_STRING') FROM pragma_user_agent()"),
	    SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch (get user_agent)", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the env handle
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Creates a table, inserts a row, selects the row, fetches the result and checks it, then disconnects and reconnects to make sure the data is still there
TEST_CASE("Connect with named file, disconnect and reconnect", "[odbc]") {
	SQLHANDLE env;
    SQLHANDLE dbc;
	SQLHANDLE hstmt = SQL_NULL_HSTMT;

    // Connect to the database using SQLConnect
    DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// create a table
	EXECUTE_AND_CHECK("SQLExecDirect (create table)", SQLExecDirect, hstmt, ConvertToSQLCHAR("CREATE OR REPLACE TABLE test_table (a INTEGER)"), SQL_NTS);

	// insert a row
	EXECUTE_AND_CHECK("SQLExecDirect (insert row)", SQLExecDirect, hstmt, ConvertToSQLCHAR("INSERT INTO test_table VALUES (1)"), SQL_NTS);

	// select the row
	EXECUTE_AND_CHECK("SQLExecDirect (select row)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test_table"), SQL_NTS);

	// Fetch the result
	EXECUTE_AND_CHECK("SQLFetch (select row)", SQLFetch, hstmt);

	// Check the result
	DATA_CHECK(hstmt, 1, "1");

    // Disconnect from the database
    DISCONNECT_FROM_DATABASE(env, dbc);

    // Reconnect to the database using SQLConnect
    DRIVER_CONNECT_TO_DATABASE(env, dbc, "Database=test_odbc_named.db");

	hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// select the row
	EXECUTE_AND_CHECK("SQLExecDirect (select row)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test_table"), SQL_NTS);

	// Fetch the result
	EXECUTE_AND_CHECK("SQLFetch (select row)", SQLFetch, hstmt);

	// Check the result
	DATA_CHECK(hstmt, 1, "1");

    // Disconnect from the database
    DISCONNECT_FROM_DATABASE(env, dbc);
}
