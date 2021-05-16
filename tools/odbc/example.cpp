#include <stdio.h>
#include <sql.h>
#include <sqlext.h>

#include <assert.h>

#define SUCCESS(x)                                                                                                     \
	do {                                                                                                               \
		SQLRETURN retval = (x);                                                                                        \
		if (retval != SQL_SUCCESS) {                                                                                   \
			fprintf(stderr, "Runtime error: %s returned %d at %s:%d\n", #x, retval, __FILE__, __LINE__);               \
			return -1;                                                                                                 \
		}                                                                                                              \
	} while (0)

// param example: "Driver=/Users/hannes/source/duckdb/build/debug/tools/odbc/libduckdb_odbc.dylib;Database=:memory:"
int main(int argc, const char **argv) {
	SQLHENV env;
	SQLHDBC dbc;
	SQLHSTMT stmt;

	/* Allocate an environment handle */
	SUCCESS(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env));

	/* We want ODBC 3 support */
	SUCCESS(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0));

	/* Allocate a connection handle */
	SUCCESS(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc));

	SUCCESS(SQLDriverConnect(dbc, NULL, (SQLCHAR *)argv[1], SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE));

	/* Allocate a statement handle */
	SUCCESS(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt));

	SUCCESS(SQLExecDirect(stmt, (SQLCHAR *)"CREATE TABLE a (i INTEGER)", SQL_NTS));
	SUCCESS(SQLExecDirect(stmt, (SQLCHAR *)"INSERT INTO a VALUES (42), (43)", SQL_NTS));

	SUCCESS(SQLPrepare(stmt, (SQLCHAR *)"SELECT i one, i+1 two, i+2 three FROM a WHERE i > ?", SQL_NTS));
	int64_t param = 41;
	SUCCESS(SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_UNKNOWN_TYPE, 0, 0, &param, 0, nullptr));
	SUCCESS(SQLExecute(stmt));

	SQLSMALLINT columns = -1; /* number of columns in result-set */

	SUCCESS(SQLNumResultCols(stmt, &columns));
	for (SQLUSMALLINT i = 1; i <= columns; i++) {
		SQLCHAR colname[100];
		SQLSMALLINT namelength;
		SUCCESS(SQLDescribeCol(stmt, i, colname, 100, &namelength, nullptr, nullptr, nullptr, nullptr));
		printf("%s\t", colname);
	}
	printf("\n");
	/* Loop through the rows in the result-set */
	while (SQLFetch(stmt) == SQL_SUCCESS) {
		/* Loop through the columns */
		for (SQLUSMALLINT i = 1; i <= columns; i++) {
			SQLLEN indicator;
			int int_res;

			/* retrieve column data as a string */
			SUCCESS(SQLGetData(stmt, i, SQL_C_SLONG, &int_res, sizeof(int), &indicator));
			printf("%d\t", int_res);
		}
		printf("\n");
	}

	SUCCESS(SQLFreeStmt(stmt, SQL_CLOSE));

	SUCCESS(SQLFreeHandle(SQL_HANDLE_STMT, stmt));
	// TODO why do those fail?
	//	SUCCESS(SQLFreeHandle(SQL_HANDLE_DBC, dbc));
	//	SUCCESS(SQLFreeHandle(SQL_HANDLE_ENV, env));
}