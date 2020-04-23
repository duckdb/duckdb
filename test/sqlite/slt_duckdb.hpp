#include <ctype.h>
#include <stdio.h>
#ifndef _MSC_VER
#include <strings.h>
#endif
#include "duckdb.h"

#include <stdlib.h>

#ifndef _WIN32
#define LLFMT "%lld"
#else
#define LLFMT "%I64d"
#endif

static duckdb_database database;

static int duckdbConnect(void *NotUsed,           /* Argument from DbEngine object.  Not used */
                         const char *zConnectStr, /* Connection string */
                         void **ppConn,           /* Write completed connection here */
                         const char *zParam       /* Value of the -parameters command-line option */
) {
	(void)NotUsed;
	(void)zConnectStr;
	(void)zParam;

	duckdb_connection conn;

	if (duckdb_open(NULL, &database) != DuckDBSuccess) {
		return 1;
	}

	if (duckdb_connect(database, &conn) != DuckDBSuccess) {
		return 1;
	}
	*ppConn = (void *)conn;
	return 0;
}

static int duckdbStatement(void *pConn,      /* Connection created by xConnect */
                           const char *zSql, /* SQL statement to evaluate */
                           int bQuiet        /* True to suppress printing errors. */
) {
	if (strncasecmp(zSql, "CREATE INDEX", 12) == 0) {
		fprintf(stderr, "Ignoring CREATE INDEX statement %s\n", zSql);
		return 0;
	}
	if (duckdb_query((duckdb_connection)pConn, (char *)zSql, NULL) != DuckDBSuccess) {
		return 1;
	}
	return 0;
}

static int duckdbQuery(void *pConn,       /* Connection created by xConnect */
                       const char *zSql,  /* SQL statement to evaluate */
                       const char *zType, /* One character for each column of result */
                       char ***pazResult, /* RETURN:  Array of result values */
                       int *pnResult      /* RETURN:  Number of result values */
) {
	duckdb_result result;

	size_t r, c;
	(void)zType;
	// fprintf(stderr, "Quack: %s\n", zSql);
	assert(pConn);
	if (duckdb_query((duckdb_connection)pConn, (char *)zSql, &result) != DuckDBSuccess) {
		return 1;
	}

	*pazResult = (char **)malloc(sizeof(char *) * result.row_count * result.column_count);
	if (!*pazResult) {
		return 1;
	}
	for (r = 0; r < result.row_count; r++) {
		for (c = 0; c < result.column_count; c++) {
			duckdb_column actual_column = result.columns[c];
			char *buffer = (char *)malloc(BUFSIZ);

			if (actual_column.nullmask[r]) {
				snprintf(buffer, BUFSIZ, "%s", "NULL");
			} else {
				switch (actual_column.type) {
				case DUCKDB_TYPE_BOOLEAN:
					snprintf(buffer, BUFSIZ, "%s", ((bool *)actual_column.data)[r] ? "1" : "0");
					break;
				case DUCKDB_TYPE_TINYINT:
					snprintf(buffer, BUFSIZ, "%d", (int)((int8_t *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_SMALLINT:
					snprintf(buffer, BUFSIZ, "%d", (int)((int16_t *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_INTEGER:
					snprintf(buffer, BUFSIZ, "%d", (int)((int32_t *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_BIGINT:
					snprintf(buffer, BUFSIZ, "%d", (int)((int64_t *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_FLOAT:
					// cast to INT seems to be the trick here
					snprintf(buffer, BUFSIZ, "%d", (int)((float *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_DOUBLE:
					// cast to INT seems to be the trick here
					snprintf(buffer, BUFSIZ, "%d", (int)((double *)actual_column.data)[r]);
					break;
				case DUCKDB_TYPE_VARCHAR: {
					char *str = ((char **)actual_column.data)[r];
					snprintf(buffer, BUFSIZ, "%s", str ? (str == 0 ? "(empty)" : str) : "NULL");
					break;
				}
				default: { fprintf(stderr, "%s\n", "UNKNOWN"); }
				}
			}
			(*pazResult)[r * result.column_count + c] = buffer;
		}
	}
	*pnResult = result.column_count * result.row_count;
	duckdb_destroy_result(&result);

	return 0;
}

static int duckdbFreeResults(void *pConn,     /* Connection created by xConnect */
                             char **azResult, /* The results to be freed */
                             int nResult      /* Number of rows of result */
) {
	int i;
	(void)pConn;
	if (!azResult) {
		return 1;
	}
	for (i = 0; i < nResult; i++) {
		if (azResult[i]) {
			free(azResult[i]);
		}
	}
	free(azResult);
	return 0;
}

static int duckdbDisconnect(void *pConn /* Connection created by xConnect */
) {
	duckdb_connection con = (duckdb_connection)pConn;
	duckdb_disconnect(&con);
	duckdb_close(&database);
	return 0;
}

static int duckdbGetEngineName(void *pConn,       /* Connection created by xConnect */
                               const char **zName /* SQL statement to evaluate */
) {
	(void)pConn;
	*zName = "DuckDB";
	return 0;
}

void registerDuckdb(void);

void registerDuckdb(void) {
	/*
	** This is the object that defines the database engine interface.
	*/
	static const DbEngine duckdbEngine = {
	    "DuckDB",            /* zName */
	    0,                   /* pAuxData */
	    duckdbConnect,       /* xConnect */
	    duckdbGetEngineName, /* xGetEngineName */
	    duckdbStatement,     /* xStatement */
	    duckdbQuery,         /* xQuery */
	    duckdbFreeResults,   /* xFreeResults */
	    duckdbDisconnect     /* xDisconnect */
	};
	sqllogictestRegisterEngine(&duckdbEngine);
}
