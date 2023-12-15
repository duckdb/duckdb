#define CATCH_CONFIG_MAIN
#include "common.h"

namespace odbc_test {

void ODBC_CHECK(SQLRETURN ret, std::string msg) {
	switch (ret) {
	case SQL_SUCCESS:
		REQUIRE(1);
		return;
	case SQL_SUCCESS_WITH_INFO:
		fprintf(stderr, "%s: Success with info\n", msg.c_str());
		break;
	case SQL_ERROR:
		fprintf(stderr, "%s: Error: Error\n", msg.c_str());
		break;
	case SQL_NO_DATA:
		fprintf(stderr, "%s: Error: no data\n", msg.c_str());
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", msg.c_str());
		break;
	default:
		fprintf(stderr, "%s: Unexpected return value\n", msg.c_str());
		break;
	}
	REQUIRE(ret == SQL_SUCCESS);
}

void ACCESS_DIAGNOSTIC(std::string &state, std::string &message, SQLHANDLE handle, SQLSMALLINT handle_type) {
	SQLCHAR sqlstate[6];
	SQLINTEGER native_error;
	SQLCHAR message_text[256];
	SQLSMALLINT text_length;
	SQLSMALLINT recnum = 0;
	SQLRETURN ret = SQL_SUCCESS;

	while (SQL_SUCCEEDED(ret)) {
		recnum++;
		// SQLGetDiagRec returns the current values of multiple fields of a diagnostic record that contains error,
		// warning, and status information.
		ret = SQLGetDiagRec(handle_type, handle, recnum, sqlstate, &native_error, message_text, sizeof(message_text),
		                    &text_length);
		// The function overwrites the previous contents of state and message so only the last diagnostic record is
		// available. Because this function usually is called on one diagnostic record, or used to confirm that two
		// calls to SQLGetDiagRec does not change the state of the statement, this is not a problem.
		if (SQL_SUCCEEDED(ret)) {
			state = ConvertToString(sqlstate);
			message = ConvertToString(message_text);
		}
	}

	if (ret != SQL_NO_DATA) {
		ODBC_CHECK(ret, "SQLGetDiagRec");
	}
}

void DATA_CHECK(HSTMT &hstmt, SQLSMALLINT col_num, const std::string expected_content) {
	SQLCHAR content[256];
	SQLLEN content_len;

	// SQLGetData returns data for a single column in the result set.
	SQLRETURN ret = SQLGetData(hstmt, col_num, SQL_C_CHAR, content, sizeof(content), &content_len);
	ODBC_CHECK(ret, "SQLGetData");
	if (content_len == SQL_NULL_DATA) {
		REQUIRE(expected_content.empty());
		return;
	}
	REQUIRE(ConvertToString(content) == expected_content);
}

void METADATA_CHECK(HSTMT &hstmt, SQLUSMALLINT col_num, const std::string &expected_col_name,
                    SQLSMALLINT expected_col_name_len, SQLSMALLINT expected_col_data_type, SQLULEN expected_col_size,
                    SQLSMALLINT expected_col_decimal_digits, SQLSMALLINT expected_col_nullable) {
	SQLCHAR col_name[256];
	SQLSMALLINT col_name_len;
	SQLSMALLINT col_type;
	SQLULEN col_size;
	SQLSMALLINT col_decimal_digits;
	SQLSMALLINT col_nullable;

	// SQLDescribeCol returns the result descriptor (column name, column size, decimal digits, and nullability) for one
	// column in a result set.
	SQLRETURN ret = SQLDescribeCol(hstmt, col_num, col_name, sizeof(col_name), &col_name_len, &col_type, &col_size,
	                               &col_decimal_digits, &col_nullable);
	ODBC_CHECK(ret, "SQLDescribeCol");

	if (!expected_col_name.empty()) {
		REQUIRE(expected_col_name.compare(ConvertToString(col_name)) == 0);
	}
	if (expected_col_name_len) {
		REQUIRE(col_name_len == expected_col_name_len);
	}
	if (expected_col_data_type) {
		REQUIRE(col_type == expected_col_data_type);
	}
	if (expected_col_size) {
		REQUIRE(col_size == expected_col_size);
	}
	if (expected_col_decimal_digits) {
		REQUIRE(col_decimal_digits == expected_col_decimal_digits);
	}
	if (expected_col_nullable) {
		REQUIRE(col_nullable == expected_col_nullable);
	}
}

void DRIVER_CONNECT_TO_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc, const std::string &extra_params) {
	std::string dsn;
	std::string default_dsn = "duckdbmemory";
	SQLCHAR str[1024];
	SQLSMALLINT strl;
	auto tmp = getenv("COMMON_CONNECTION_STRING_FOR_REGRESSION_TEST");
	std::string envvar = tmp ? tmp : "";

	if (!envvar.empty()) {
		if (!extra_params.empty()) {
			dsn = "DSN=" + default_dsn + ";" + extra_params + ";" + envvar + ";" + extra_params;
		} else {
			dsn = "DSN=" + default_dsn + ";" + envvar;
		}
	} else {
		if (!extra_params.empty()) {
			dsn = "DSN=" + default_dsn + ";" + extra_params;
		} else {
			dsn = "DSN=" + default_dsn;
		}
	}

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	// SQLDriverConnect establishes connections to a driver and a data source.
	// Supports data sources that require more connection information than the three arguments in SQLConnect.
	EXECUTE_AND_CHECK("SQLDriverConnect", SQLDriverConnect, dbc, nullptr, ConvertToSQLCHAR(dsn.c_str()), SQL_NTS, str,
	                  sizeof(str), &strl, SQL_DRIVER_COMPLETE);
}

void CONNECT_TO_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc) {
	std::string dsn = "DuckDB";

	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);

	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	// SQLConnect establishes connections to a driver and a data source.
	EXECUTE_AND_CHECK("SQLConnect", SQLConnect, dbc, ConvertToSQLCHAR(dsn.c_str()), SQL_NTS, nullptr, 0, nullptr, 0);
}

void DISCONNECT_FROM_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc) {
	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_ENV)", SQLFreeHandle, SQL_HANDLE_ENV, env);

	EXECUTE_AND_CHECK("SQLDisconnect", SQLDisconnect, dbc);

	EXECUTE_AND_CHECK("SQLFreeHandle(SQL_HANDLE_DBC)", SQLFreeHandle, SQL_HANDLE_DBC, dbc);
}

void EXEC_SQL(HSTMT hstmt, const std::string &query) {
	EXECUTE_AND_CHECK("SQLExecDirect (" + query + ")", SQLExecDirect, hstmt, ConvertToSQLCHAR(query.c_str()), SQL_NTS);
}

void InitializeDatabase(HSTMT &hstmt) {
	EXEC_SQL(hstmt, "DROP TABLE IF EXISTS test_table_1;");
	EXEC_SQL(hstmt, "CREATE TABLE test_table_1 (id integer PRIMARY KEY, t varchar(20));");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (1, 'foo');");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (2, 'bar');");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (3, 'foobar');");

	EXEC_SQL(hstmt, "DROP TABLE IF EXISTS bool_table;");
	EXEC_SQL(hstmt, "CREATE TABLE bool_table (id integer, t varchar(5), b boolean);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (1, 'yeah', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (2, 'yes', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (3, 'true', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (4, 'false', false);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (5, 'not', false);");

	EXEC_SQL(hstmt, "DROP TABLE IF EXISTS bytea_table;");
	EXEC_SQL(hstmt, "CREATE TABLE bytea_table (id integer, t blob);");
	EXEC_SQL(hstmt, "INSERT INTO bytea_table VALUES (1, '\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x10'::blob);");
	EXEC_SQL(hstmt, "INSERT INTO bytea_table VALUES (2, 'bar');");
	EXEC_SQL(hstmt, "INSERT INTO bytea_table VALUES (3, 'foobar');");
	EXEC_SQL(hstmt, "INSERT INTO bytea_table VALUES (4, 'foo');");
	EXEC_SQL(hstmt, "INSERT INTO bytea_table VALUES (5, 'barf');");

	EXEC_SQL(hstmt, "DROP TABLE IF EXISTS interval_table;");
	EXEC_SQL(hstmt, "CREATE TABLE interval_table(id integer, iv interval, d varchar(100));");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (1, '1 day', 'one day');");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (2, '10 seconds', 'ten secs');");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (3, '100 years', 'hundred years');");

	EXEC_SQL(hstmt, "CREATE VIEW test_view AS SELECT * FROM test_table_1;");

	EXEC_SQL(hstmt, "CREATE TABLE lo_test_table (id int4, large_data blob);");
}

std::map<SQLSMALLINT, SQLULEN> InitializeTypesMap() {
	std::map<SQLSMALLINT, SQLULEN> types_map;

	types_map[SQL_VARCHAR] = 256;
	types_map[SQL_CHAR] = 0;
	types_map[SQL_BIGINT] = 20;
	types_map[SQL_INTEGER] = 11;
	types_map[SQL_SMALLINT] = 5;
	return types_map;
}

SQLCHAR *ConvertToSQLCHAR(const char *str) {
	return reinterpret_cast<SQLCHAR *>(const_cast<char *>(str));
}

SQLCHAR *ConvertToSQLCHAR(const std::string &str) {
	return reinterpret_cast<SQLCHAR *>(const_cast<char *>(str.c_str()));
}

std::string ConvertToString(SQLCHAR *str) {
	return std::string(reinterpret_cast<char *>(str));
}

const char *ConvertToCString(SQLCHAR *str) {
	return reinterpret_cast<const char *>(str);
}

SQLPOINTER ConvertToSQLPOINTER(uint64_t ptr) {
	return reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(ptr));
}

SQLPOINTER ConvertToSQLPOINTER(const char *str) {
	return reinterpret_cast<SQLPOINTER>(const_cast<char *>(str));
}

std::string ConvertHexToString(SQLCHAR val[16], int precision) {
	std::stringstream ss;
	ss << std::hex << std::uppercase << std::setfill('0');
	for (int i = 0; i < precision; i++) {
		ss << std::setw(2) << static_cast<unsigned int>(val[i]);
	}
	return ss.str().substr(0, precision);
}

} // namespace odbc_test
