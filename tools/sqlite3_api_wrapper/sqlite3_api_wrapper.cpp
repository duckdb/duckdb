#include "sqlite3.h"

#include <ctype.h>
#include <duckdb.h>
#include <err.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <stdlib.h>

using namespace std;

// TODO:
#define SOURCE_ID __DATE__
#define LIB_VERSION "0.01"

#define NOT_IMPLEMENTED(func_decl)                                                                                     \
	func_decl {                                                                                                        \
		fprintf(stderr, "The function " #func_decl " of the sqlite3 api has not been implemented!\n");                 \
		exit(1);                                                                                                       \
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
	string last_error;
};

void sqlite3_randomness(int N, void *pBuf) {
	static bool init = false;
	if (!init) {
		srand(time(NULL));
		init = true;
	}
	unsigned char *zBuf = (unsigned char *)pBuf;
	while (N--) {
		unsigned char nextByte = rand() % 255;
		zBuf[N] = nextByte;
	}
}

int sqlite3_open(const char *filename, /* Database filename (UTF-8) */
                 sqlite3 **ppDb        /* OUT: SQLite db handle */
) {
	duckdb_database database_ptr;
	duckdb_connection connection_ptr;

	if (strcmp(filename, ":memory:") == 0) {
		filename = NULL;
	}

	if (duckdb_open(filename, &database_ptr) != DuckDBSuccess) {
		fprintf(stderr, "Couldn't open the database file: %s\n", filename);
		exit(1);
	}
	if (duckdb_connect(database_ptr, &connection_ptr) != DuckDBSuccess) {
		fprintf(stderr, "Couldn't connect to the database.\n");
		exit(1);
	}

	sqlite3 *pDb = (sqlite3 *)malloc(sizeof(sqlite3));
	if (!pDb)
		err(1, NULL);

	pDb->pDuckDB = database_ptr;
	pDb->pCon = connection_ptr;
	*ppDb = pDb;

	return SQLITE_OK;
}

/* In SQLite this function compiles the query into VDBE bytecode,
 * in the implementation it currently executes the query */
// TODO: prepare the statement instead of executing right away
int sqlite3_prepare_v2(sqlite3 *db,           /* Database handle */
                       const char *zSql,      /* SQL statement, UTF-8 encoded */
                       int nByte,             /* Maximum length of zSql in bytes. */
                       sqlite3_stmt **ppStmt, /* OUT: Statement handle */
                       const char **pzTail    /* OUT: Pointer to unused portion of zSql */
) {
	sqlite3_stmt *pStmt = (sqlite3_stmt *)malloc(sizeof(sqlite3_stmt));
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

	if ((pStmt->zSql = (char *)malloc(size)) == NULL)
		err(1, NULL);

	/* Init sql statement */
	pStmt->zSql[size - 1] = '\0';
	strncpy(pStmt->zSql, zSql, size - 1);
	pStmt->lenSql = size - 1;

	/* Execute the query */
	pStmt->curr_row = -1;
	*ppStmt = pStmt;
	if (duckdb_query(db->pCon, pStmt->zSql, &pStmt->result) != DuckDBSuccess) {
		if (pStmt->result.error_message) {
			db->last_error = string(pStmt->result.error_message);
		}
		return SQLITE_ERROR;
	}
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
int sqlite3_exec(sqlite3 *db,                /* The database on which the SQL executes */
                 const char *zSql,           /* The SQL to be executed */
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
			if (pStmt) {
				*pzErrMsg = pStmt->result.error_message;
				pStmt->result.error_message = 0;
			}
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
			if (xCallback && (SQLITE_ROW == rc || (SQLITE_DONE == rc && !callbackIsInit))) {
				if (!callbackIsInit) {
					azCols = (char **)malloc((2 * nCol + 1) * sizeof(const char *));
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
						if (!azVals[i] && sqlite3_column_type(pStmt, i) != SQLITE_NULL) {
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
	if (rc != SQLITE_OK && !*pzErrMsg) {
		// error but no error message set
		static const char *unknown_error = "Unknown error in DuckDB!";
		*pzErrMsg = (char *)sqlite3_malloc64(strlen(unknown_error) + 1);
		strcpy(*pzErrMsg, unknown_error);
	}

	return rc;
}

int sqlite3_close(sqlite3 *db) {
	duckdb_disconnect(&db->pCon);
	duckdb_close(&db->pDuckDB);
	return SQLITE_OK;
}

/* Return the text of the SQL that was used to prepare the statement */
const char *sqlite3_sql(sqlite3_stmt *pStmt) {
	char *zSql = (char *)malloc(pStmt->lenSql + 1);
	strncpy(zSql, pStmt->zSql, pStmt->lenSql + 1);
	return zSql;
}

int sqlite3_column_count(sqlite3_stmt *pStmt) {
	return (int)pStmt->result.column_count;
}

int sqlite3_column_type(sqlite3_stmt *pStmt, int iCol) {
	duckdb_column column = pStmt->result.columns[iCol];

	if (pStmt->result.columns[iCol].nullmask[pStmt->curr_row])
		return SQLITE_NULL;

	switch (column.type) {
	case DUCKDB_TYPE_BOOLEAN:
	case DUCKDB_TYPE_TINYINT:
	case DUCKDB_TYPE_SMALLINT:
	case DUCKDB_TYPE_INTEGER:
	case DUCKDB_TYPE_BIGINT: /* TODO: Maybe blob? */
		return SQLITE_INTEGER;
	case DUCKDB_TYPE_DOUBLE:
		return SQLITE_FLOAT;
	case DUCKDB_TYPE_DATE:
	case DUCKDB_TYPE_VARCHAR:
		return SQLITE_BLOB;
	default:
		return 0;
	}
	return 0;
}

const char *sqlite3_column_name(sqlite3_stmt *pStmt, int N) {
	return pStmt->result.columns[N].name;
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int iCol) {
	if (iCol >= pStmt->result.column_count)
		return NULL;
	if (pStmt->result.columns[iCol].nullmask[pStmt->curr_row]) {
		return NULL;
	}
	return (unsigned char *)duckdb_value_varchar(&pStmt->result, iCol, pStmt->curr_row);
}

int sqlite3_initialize(void) {
	return SQLITE_OK;
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
	if (pStmt != NULL) {
		duckdb_destroy_result(&pStmt->result);
		free(pStmt->zSql);
		free(pStmt);
	}
	return SQLITE_OK;
}

/*
** Some systems have stricmp().  Others have strcasecmp().  Because
** there is no consistency, we will define our own.
**
** IMPLEMENTATION-OF: R-30243-02494 The sqlite3_stricmp() and
** sqlite3_strnicmp() APIs allow applications and extensions to compare
** the contents of two buffers containing UTF-8 strings in a
** case-independent fashion, using the same definition of "case
** independence" that SQLite uses internally when comparing identifiers.
*/

const unsigned char sqlite3UpperToLower[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
    22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
    44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  97,
    98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
    132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
    154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
    198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
    220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
    242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};

int sqlite3StrICmp(const char *zLeft, const char *zRight) {
	unsigned char *a, *b;
	int c;
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	for (;;) {
		c = (int)sqlite3UpperToLower[*a] - (int)sqlite3UpperToLower[*b];
		if (c || *a == 0)
			break;
		a++;
		b++;
	}
	return c;
}

SQLITE_API int sqlite3_stricmp(const char *zLeft, const char *zRight) {
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	return sqlite3StrICmp(zLeft, zRight);
}

SQLITE_API int sqlite3_strnicmp(const char *zLeft, const char *zRight, int N) {
	unsigned char *a, *b;
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	while (N-- > 0 && *a != 0 && sqlite3UpperToLower[*a] == sqlite3UpperToLower[*b]) {
		a++;
		b++;
	}
	return N < 0 ? 0 : sqlite3UpperToLower[*a] - sqlite3UpperToLower[*b];
}

void *sqlite3_malloc64(sqlite3_uint64 n) {
	return malloc(n);
}
void sqlite3_free(void *pVoid) {
	free(pVoid);
}

// TODO: test
/* Printf into a newly allocated buffer */
char *sqlite3_mprintf(const char *fmt, ...) {
	size_t str_size = strlen(fmt) + 50;
	char *res = (char *)malloc(str_size);
	if (res == NULL)
		err(1, NULL);

	va_list valist;
	va_start(valist, fmt);

	size_t str_len = (size_t)vsnprintf(res, str_size - 1, fmt, valist);
	if (str_len >= str_size) {
		str_size = str_len + 1;
		res = (char *)realloc(res, str_size);
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
int sqlite3_config(int i, ...) {
	return SQLITE_OK;
}

// TODO: stub
int sqlite3_errcode(sqlite3 *db) {
	return SQLITE_OK;
}

// TODO: stub
const char *sqlite3_errmsg(sqlite3 *db) {
	return db->last_error.c_str();
}

// TODO: stub
void sqlite3_interrupt(sqlite3 *db){};

const char *sqlite3_libversion(void) {
	return LIB_VERSION;
}
const char *sqlite3_sourceid(void) {
	return SOURCE_ID;
}
