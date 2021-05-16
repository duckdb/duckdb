#include <stdio.h>
#include <sql.h>
#include <sqlext.h>

/*
 * see Retrieving ODBC Diagnostics
 * for a definition of extract_error().
 */
void extract_error(const char *fn, SQLHANDLE handle, SQLSMALLINT type) {
	SQLINTEGER i = 0;
	SQLINTEGER native;
	SQLCHAR state[7];
	SQLCHAR text[256];
	SQLSMALLINT len;
	SQLRETURN ret;

	fprintf(stderr,
	        "\n"
	        "The driver reported the following diagnostics whilst running "
	        "%s\n\n",
	        fn);

	do {
		ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text), &len);
		if (SQL_SUCCEEDED(ret))
			printf("%s:%d:%d:%s\n", state, i, native, text);
	} while (ret == SQL_SUCCESS);
}

// param example: "Driver=/Users/hannes/source/duckdb/build/debug/tools/odbc/libduckdb_odbc.dylib;Database=:memory:"
int main(int argc, const char **argv) {
	SQLHENV env;
	SQLHDBC dbc;
	SQLHSTMT stmt;
	SQLRETURN ret;

	/* Allocate an environment handle */
	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

	/* We want ODBC 3 support */
	SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);

	/* Allocate a connection handle */
	SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

	/* Connect to the DSN mydsn */
	/* You will need to change mydsn to one you have created */
	/* and tested */
	SQLDriverConnect(dbc, NULL, (SQLCHAR *)argv[1], SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);

	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	/* Retrieve a list of tables */

	SQLExecDirect(stmt, (SQLCHAR *)"create table a (i integer)", SQL_NTS);
	SQLExecDirect(stmt, (SQLCHAR *)"insert into a values (42), (43)", SQL_NTS);

	SQLSMALLINT columns; /* number of columns in result-set */

	// SQLBindCol?

	SQLPrepare(stmt, (SQLCHAR *)"SELECT i, i+1, i+2 FROM a", SQL_NTS);
	SQLExecute(stmt);

	SQLNumResultCols(stmt, &columns);

	printf("num_cols=%d\n", columns);
	/* Loop through the rows in the result-set */
	while (SQLFetch(stmt) == SQL_SUCCESS) {
		SQLUSMALLINT i;
		/* Loop through the columns */
		for (i = 1; i <= columns; i++) {
			SQLLEN indicator;
			int int_res;
			/* retrieve column data as a string */
			ret = SQLGetData(stmt, i, SQL_C_SLONG, &int_res, sizeof(int), &indicator);
			if (ret == SQL_SUCCESS) {
				/* Handle null columns */
				// if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
				printf("  Column %u : %d\n", i, int_res);
			}
		}
	}

	SQLFreeHandle(SQL_HANDLE_STMT, &stmt);
	SQLFreeHandle(SQL_HANDLE_DBC, &dbc);
	SQLFreeHandle(SQL_HANDLE_ENV, &env);

	printf("OK\n");
}