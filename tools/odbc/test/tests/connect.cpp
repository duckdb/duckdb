#include "../common.h"

using namespace odbc_test;

TEST_CASE("Test SQLConnect and SQLDriverConnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
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
