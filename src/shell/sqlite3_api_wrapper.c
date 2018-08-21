#include "sqlite3.h"
#include <ctype.h>
#include <duckdb.h>
#include <err.h>
#include <stdio.h>
#include <string.h>

// TODO:
#define SOURCE_ID __DATE__" "__TIME__
#define LIB_VERSION "0.01"

#define NOT_IMPLEMENTED(func_decl)                                             \
	func_decl {                                                                \
		fprintf(stderr, "The function " #func_decl                             \
		                " of the sqlite3 api has not been implemented!\n");    \
		exit(1);                                                               \
	}

struct sqlite3_stmt {
	char *zSql;
	size_t lenSql;
	duckdb_result result;
	int curr_row;
};

struct sqlite3 {
	duckdb_database pDuckDB;
	duckdb_connection pCon;
};

int sqlite3_open(const char *filename, /* Database filename (UTF-8) */
                 sqlite3 **ppDb        /* OUT: SQLite db handle */
) {
	duckdb_database database_ptr;
	duckdb_connection connection_ptr;

	if (strcmp(filename, ":memory:") == 0) {
		filename = NULL;
	}

	if (duckdb_open(filename, &database_ptr) != DuckDBSuccess) {
		fprintf(stderr, "Couldn't open the database file: %s", filename);
		exit(1);
	}
	if (duckdb_connect(database_ptr, &connection_ptr) != DuckDBSuccess) {
		fprintf(stderr, "Couldn't connect to the database.");
		exit(1);
	}

	sqlite3 *pDb;
	if ((pDb = malloc(sizeof(sqlite3))) == NULL)
		err(1, NULL);

	pDb->pDuckDB = database_ptr;
	pDb->pCon = connection_ptr;
	*ppDb = pDb;

	return SQLITE_OK;
}

/* In SQLite this function compiles the query into VDBE bytecode,
 * in the implementation it currently executes the query */
// TODO: prepare the statement instead of executing right away
int sqlite3_prepare_v2(
    sqlite3 *db,           /* Database handle */
    const char *zSql,      /* SQL statement, UTF-8 encoded */
    int nByte,             /* Maximum length of zSql in bytes. */
    sqlite3_stmt **ppStmt, /* OUT: Statement handle */
    const char **pzTail    /* OUT: Pointer to unused portion of zSql */
) {
	sqlite3_stmt *pStmt = malloc(sizeof(sqlite3_stmt));
	if (pStmt == NULL)
		err(1, NULL);

	size_t size = 1;

	/* Set the tail to the end of the first SQL statement */
	for (*pzTail = zSql; **pzTail && **pzTail != ';'; (*pzTail)++)
		size++;
	if (**pzTail == ';') {
		size++;
		(*pzTail)++;
	}

	if ((pStmt->zSql = malloc(size)) == NULL)
		err(1, NULL);

	/* Init sql statement */
	strncpy(pStmt->zSql, zSql, size);
	pStmt->lenSql = size - 1;

	/* Execute the query */
	pStmt->curr_row = -1;
	if (duckdb_query(db->pCon, pStmt->zSql, &pStmt->result) != DuckDBSuccess)
		return SQLITE_ERROR;

	*ppStmt = pStmt;
	return SQLITE_OK;
}

/* Prepare the next result to be retrieved */
int sqlite3_step(sqlite3_stmt *pStmt) {
	pStmt->curr_row++;
	if (pStmt->curr_row >= pStmt->result.row_count)
		return SQLITE_DONE;

	return SQLITE_ROW;
}

/* Execute multiple semicolon separated SQL statements
 * and execute the passed callback for each produced result,
 * largely copied from the original sqlite3 source */
int sqlite3_exec(sqlite3 *db,      /* The database on which the SQL executes */
                 const char *zSql, /* The SQL to be executed */
                 sqlite3_callback xCallback, /* Invoke this callback routine */
                 void *pArg,                 /* First argument to xCallback() */
                 char **pzErrMsg             /* Write error messages here */
) {
	int rc = SQLITE_OK;      /* Return code */
	const char *zLeftover;   /* Tail of unprocessed SQL */
	sqlite3_stmt *pStmt = 0; /* The current SQL statement */
	char **azCols = 0;       /* Names of result columns */
	int callbackIsInit;      /* True if callback data is initialized */

	if (zSql == 0)
		zSql = "";

	while (rc == SQLITE_OK && zSql[0]) {
		int nCol;
		char **azVals = 0;

		pStmt = 0;
		rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
		if (rc != SQLITE_OK) {
			continue;
		}
		if (!pStmt) {
			/* this happens for a comment or white-space */
			zSql = zLeftover;
			continue;
		}

		callbackIsInit = 0;
		nCol = sqlite3_column_count(pStmt);

		while (1) {
			int i;
			rc = sqlite3_step(pStmt);

			/* Invoke the callback function if required */
			if (xCallback &&
			    (SQLITE_ROW == rc || (SQLITE_DONE == rc && !callbackIsInit))) {
				if (!callbackIsInit) {
					azCols = malloc((2 * nCol + 1) * sizeof(const char *));
					if (azCols == NULL) {
						goto exec_out;
					}
					for (i = 0; i < nCol; i++) {
						azCols[i] = (char *)sqlite3_column_name(pStmt, i);
					}
					callbackIsInit = 1;
				}
				if (rc == SQLITE_ROW) {
					azVals = &azCols[nCol];
					for (i = 0; i < nCol; i++) {
						azVals[i] = (char *)sqlite3_column_text(pStmt, i);
						if (!azVals[i] &&
						    sqlite3_column_type(pStmt, i) != SQLITE_NULL) {
							fprintf(stderr, "sqlite3_exec: out of memory.\n");
							goto exec_out;
						}
					}
					azVals[i] = 0;
				}
				if (xCallback(pArg, nCol, azVals, azCols)) {
					/* EVIDENCE-OF: R-38229-40159 If the callback function to
					** sqlite3_exec() returns non-zero, then sqlite3_exec() will
					** return SQLITE_ABORT. */
					rc = SQLITE_ABORT;
					sqlite3_finalize(pStmt);
					pStmt = 0;
					fprintf(stderr, "sqlite3_exec: callback returned non-zero. "
					                "Aborting.\n");
					goto exec_out;
				}
			}

			if (rc != SQLITE_ROW) {
				rc = sqlite3_finalize(pStmt);
				pStmt = 0;
				zSql = zLeftover;
				while (isspace(zSql[0]))
					zSql++;
				break;
			}
		}

		free(azCols);
		azCols = 0;
	}

exec_out:
	if (pStmt)
		sqlite3_finalize(pStmt);
	free(azCols);

	return rc;
}

int sqlite3_close(sqlite3 *db) {
	if (duckdb_disconnect(db->pCon) != DuckDBSuccess ||
	    duckdb_close(db->pDuckDB) != DuckDBSuccess)
		return SQLITE_ERROR;

	return SQLITE_OK;
}

/* Return the text of the SQL that was used to prepare the statement */
const char *sqlite3_sql(sqlite3_stmt *pStmt) {
	char *zSql = malloc(pStmt->lenSql + 1);
	strncpy(zSql, pStmt->zSql, pStmt->lenSql + 1);
	return zSql;
}

int sqlite3_column_count(sqlite3_stmt *pStmt) {
	return (int)pStmt->result.column_count;
}

int sqlite3_column_type(sqlite3_stmt *pStmt, int iCol) {
	duckdb_column column = pStmt->result.columns[iCol];

	if (duckdb_value_is_null(pStmt->result.columns[iCol], pStmt->curr_row))
		return SQLITE_NULL;

	switch (column.type) {
	case DUCKDB_TYPE_BOOLEAN:
	case DUCKDB_TYPE_TINYINT:
	case DUCKDB_TYPE_SMALLINT:
	case DUCKDB_TYPE_INTEGER:
	case DUCKDB_TYPE_BIGINT: /* TODO: Maybe blob? */
		return SQLITE_INTEGER;
	case DUCKDB_TYPE_DECIMAL:
		return SQLITE_FLOAT;
	case DUCKDB_TYPE_POINTER:
	case DUCKDB_TYPE_DATE:
	case DUCKDB_TYPE_VARCHAR:
		return SQLITE_BLOB;
	}
	return 0;
}

// TODO: add actual names
const char *sqlite3_column_name(sqlite3_stmt *pStmt, int N) {
	return sqlite3_mprintf("%d", N);
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int iCol) {
	if (iCol >= pStmt->result.column_count)
		return NULL;
	return (unsigned char *)duckdb_get_value_str(pStmt->result.columns[iCol],
	                                             pStmt->curr_row);
}

int sqlite3_initialize(void) { return SQLITE_OK; }

int sqlite3_finalize(sqlite3_stmt *pStmt) {
	duckdb_destroy_result(pStmt->result);
	free(pStmt->zSql);
	free(pStmt);
	return SQLITE_OK;
}

/* Case insensitive string comparison */
int sqlite3_stricmp(const char *s1, const char *s2) {
	for (; *s1 == *s2; ++s1, ++s2)
		if (*s1 == 0)
			return 0;
	return tolower(*s1) < tolower(*s2) ? -1 : 1;
}

void *sqlite3_malloc64(sqlite3_uint64 n) { return malloc(n); }
void sqlite3_free(void *pVoid) { free(pVoid); }

// TODO: test
/* Printf into a newly allocated buffer */
char *sqlite3_mprintf(const char *fmt, ...) {
	size_t str_size = strlen(fmt) + 50;
	char *res = malloc(str_size);
	if (res == NULL)
		err(1, NULL);

	va_list valist;
	va_start(valist, fmt);

	size_t str_len = (size_t)vsnprintf(res, str_size - 1, fmt, valist);
	if (str_len >= str_size) {
		str_size = str_len + 1;
		res = realloc(res, str_size);
		va_start(valist, fmt);
		vsnprintf(res, str_size - 1, fmt, valist);
	}
	va_end(valist);

	return res;
}

char *sqlite3_snprintf(int size, char *str, const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	vsnprintf(str, size, fmt, args);
	va_end(args);
	return str;
}

// TODO: stub
int sqlite3_config(int i, ...) { return SQLITE_OK; }

// TODO: stub
int sqlite3_errcode(sqlite3 *db) { return SQLITE_OK; }

// TODO: stub
const char *sqlite3_errmsg(sqlite3 *db) { return "Unknown error"; }

// TODO: stub
void sqlite3_interrupt(sqlite3 *db){};

const char *sqlite3_libversion(void) { return LIB_VERSION; }
const char *sqlite3_sourceid(void) { return SOURCE_ID; }
