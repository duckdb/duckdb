#include "sqlite3.h"

#include <ctype.h>
#include <duckdb.hpp>
#include <err.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <stdlib.h>

using namespace duckdb;
using namespace std;

#define SOURCE_ID __DATE__
#define LIB_VERSION "DuckDB 0.1"

static char *sqlite3_strdup(const char *str);

struct sqlite3_string_buffer {
	//! String data
	unique_ptr<char[]> data;
};

struct sqlite3_stmt {
	//! The DB object that this statement belongs to
	sqlite3 *db;
	//! The query string
	string query_string;
	//! The prepared statement object, if successfully prepared
	unique_ptr<PreparedStatement> prepared;
	//! The result object, if successfully executed
	unique_ptr<QueryResult> result;
	//! The current chunk that we are iterating over
	unique_ptr<DataChunk> current_chunk;
	//! The current row into the current chunk that we are iterating over
	int64_t current_row;
	//! Bound values, used for binding to the prepared statement
	vector<Value> bound_values;
	//! Names of the prepared parameters
	vector<string> bound_names;
	//! The current column values converted to string, used and filled by sqlite3_column_text
	unique_ptr<sqlite3_string_buffer[]> current_text;
};

struct sqlite3 {
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
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
	if (strcmp(filename, ":memory:") == 0) {
		filename = NULL;
	}
	*ppDb = nullptr;

	sqlite3 *pDb = nullptr;
	try {
		pDb = new sqlite3();
		pDb->db = make_unique<DuckDB>(filename);
		pDb->con = make_unique<Connection>(*pDb->db);
	} catch (std::exception &ex) {
		if (pDb) {
			pDb->last_error = ex.what();
		}
		return SQLITE_ERROR;
	}
	*ppDb = pDb;
	return SQLITE_OK;
}

int sqlite3_close(sqlite3 *db) {
	if (db) {
		delete db;
	}
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
	if (!db || !ppStmt || !zSql) {
		return SQLITE_MISUSE;
	}
	*ppStmt = nullptr;
	string query = nByte < 0 ? zSql : string(zSql, nByte);
	if (pzTail) {
		*pzTail = zSql + query.size();
	}

	try {
		// extract the statements from the SQL query
		auto statements = db->con->ExtractStatements(query);
		if (statements.size() == 0) {
			// no statements to prepare!
			return SQLITE_OK;
		}

		// extract the first statement
		auto statement = statements[0].get();
		// extract the remainder
		bool set_remainder = statement->stmt_location + statement->stmt_length < query.size();
		query = query.substr(statement->stmt_location, statement->stmt_length);

		// now prepare the query
		auto prepared = db->con->Prepare(query);
		if (!prepared->success) {
			// failed to prepare: set the error message
			db->last_error = prepared->error;
			return SQLITE_ERROR;
		}

		// create the statement entry
		unique_ptr<sqlite3_stmt> stmt = make_unique<sqlite3_stmt>();
		stmt->db = db;
		stmt->query_string = query;
		stmt->prepared = move(prepared);
		stmt->current_row = -1;
		for (index_t i = 0; i < stmt->prepared->n_param; i++) {
			stmt->bound_names.push_back("$" + to_string(i + 1));
			stmt->bound_values.push_back(Value());
		}

		// extract the remainder of the query and assign it to the pzTail
		if (pzTail && set_remainder) {
			*pzTail = zSql + query.size() + 1;
		}

		*ppStmt = stmt.release();
		return SQLITE_OK;
	} catch (std::exception &ex) {
		db->last_error = ex.what();
		return SQLITE_ERROR;
	}
}

bool sqlite3_display_result(StatementType type) {
	switch(type) {
	case StatementType::EXECUTE:
	case StatementType::EXPLAIN:
	case StatementType::SELECT:
		return true;
	default:
		return false;
	}
}

/* Prepare the next result to be retrieved */
int sqlite3_step(sqlite3_stmt *pStmt) {
	if (!pStmt) {
		return SQLITE_MISUSE;
	}
	if (!pStmt->prepared) {
		pStmt->db->last_error = "Attempting sqlite3_step() on a non-successfully prepared statement";
		return SQLITE_ERROR;
	}
	pStmt->current_text = nullptr;
	if (!pStmt->result) {
		// no result yet! call Execute()
		pStmt->result = pStmt->prepared->Execute(pStmt->bound_values);
		if (!pStmt->result->success) {
			// error in execute: clear prepared statement
			pStmt->db->last_error = pStmt->result->error;
			pStmt->prepared = nullptr;
			return SQLITE_ERROR;
		}
		// fetch a chunk
		pStmt->current_chunk = pStmt->result->Fetch();
		pStmt->current_row = -1;
		if (!sqlite3_display_result(pStmt->prepared->type)) {
			// only SELECT statements return results
			sqlite3_reset(pStmt);
		}
	}
	if (!pStmt->current_chunk) {
		return SQLITE_DONE;
	}
	pStmt->current_row++;
	if (pStmt->current_row >= (int32_t)pStmt->current_chunk->size()) {
		// have to fetch again!
		pStmt->current_row = 0;
		pStmt->current_chunk = pStmt->result->Fetch();
		if (!pStmt->current_chunk || pStmt->current_chunk->size() == 0) {
			sqlite3_reset(pStmt);
			return SQLITE_DONE;
		}
	}
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
	int rc = SQLITE_OK;            /* Return code */
	const char *zLeftover;         /* Tail of unprocessed SQL */
	sqlite3_stmt *pStmt = nullptr; /* The current SQL statement */
	char **azCols = nullptr;       /* Names of result columns */
	char **azVals = nullptr;       /* Result values */

	if (zSql == nullptr) {
		zSql = "";
	}

	while (rc == SQLITE_OK && zSql[0]) {
		int nCol;

		pStmt = nullptr;
		rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
		if (rc != SQLITE_OK) {
			if (pzErrMsg) {
				auto errmsg = sqlite3_errmsg(db);
				*pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : nullptr;
			}
			continue;
		}
		if (!pStmt) {
			/* this happens for a comment or white-space */
			zSql = zLeftover;
			continue;
		}

		nCol = sqlite3_column_count(pStmt);
		azCols = (char **)malloc(nCol * sizeof(const char *));
		azVals = (char **)malloc(nCol * sizeof(const char *));
		if (!azCols || !azVals) {
			goto exec_out;
		}
		for (int i = 0; i < nCol; i++) {
			azCols[i] = (char *)sqlite3_column_name(pStmt, i);
		}

		while (true) {
			rc = sqlite3_step(pStmt);

			/* Invoke the callback function if required */
			if (xCallback && rc == SQLITE_ROW) {
				for (int i = 0; i < nCol; i++) {
					azVals[i] = (char *)sqlite3_column_text(pStmt, i);
					if (!azVals[i] && sqlite3_column_type(pStmt, i) != SQLITE_NULL) {
						fprintf(stderr, "sqlite3_exec: out of memory.\n");
						goto exec_out;
					}
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
			if (rc == SQLITE_DONE) {
				rc = sqlite3_finalize(pStmt);
				pStmt = nullptr;
				zSql = zLeftover;
				while (isspace(zSql[0]))
					zSql++;
				break;
			} else if (rc != SQLITE_ROW) {
				// error
				if (pzErrMsg) {
					auto errmsg = sqlite3_errmsg(db);
					*pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : nullptr;
				}
				goto exec_out;
			}
		}

		sqlite3_free(azCols);
		sqlite3_free(azVals);
		azCols = nullptr;
		azVals = nullptr;
	}

exec_out:
	if (pStmt) {
		sqlite3_finalize(pStmt);
	}
	sqlite3_free(azCols);
	sqlite3_free(azVals);
	if (rc != SQLITE_OK && pzErrMsg && !*pzErrMsg) {
		// error but no error message set
		*pzErrMsg = sqlite3_strdup("Unknown error in DuckDB!");
	}
	return rc;
}

/* Return the text of the SQL that was used to prepare the statement */
const char *sqlite3_sql(sqlite3_stmt *pStmt) {
	return pStmt->query_string.c_str();
}

int sqlite3_column_count(sqlite3_stmt *pStmt) {
	if (!pStmt) {
		return 0;
	}
	return (int)pStmt->prepared->types.size();
}

////////////////////////////
//     sqlite3_column     //
////////////////////////////
int sqlite3_column_type(sqlite3_stmt *pStmt, int iCol) {
	if (!pStmt || !pStmt->result || !pStmt->current_chunk) {
		return 0;
	}
	if (pStmt->current_chunk->data[iCol].nullmask[pStmt->current_row]) {
		return SQLITE_NULL;
	}
	auto column_type = pStmt->result->sql_types[iCol];
	switch (column_type.id) {
	case SQLTypeId::BOOLEAN:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT: /* TODO: Maybe blob? */
		return SQLITE_INTEGER;
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLITE_FLOAT;
	case SQLTypeId::DATE:
	case SQLTypeId::TIME:
	case SQLTypeId::TIMESTAMP:
	case SQLTypeId::VARCHAR:
		return SQLITE_BLOB;
	default:
		return 0;
	}
	return 0;
}

const char *sqlite3_column_name(sqlite3_stmt *pStmt, int N) {
	if (!pStmt) {
		return nullptr;
	}
	return pStmt->prepared->names[N].c_str();
}

static bool sqlite3_column_has_value(sqlite3_stmt *pStmt, int iCol, SQLType target_type, Value &val) {
	if (!pStmt || !pStmt->result || !pStmt->current_chunk) {
		return false;
	}
	if (iCol < 0 || iCol >= (int)pStmt->result->sql_types.size()) {
		return false;
	}
	if (pStmt->current_chunk->data[iCol].nullmask[pStmt->current_row]) {
		return false;
	}
	try {
		val = pStmt->current_chunk->data[iCol]
		          .GetValue(pStmt->current_row)
		          .CastAs(pStmt->result->sql_types[iCol], target_type);
	} catch (...) {
		return false;
	}
	return true;
}

double sqlite3_column_double(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, SQLTypeId::DOUBLE, val)) {
		return 0;
	}
	return val.value_.double_;
}

int sqlite3_column_int(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, SQLTypeId::INTEGER, val)) {
		return 0;
	}
	return val.value_.integer;
}

sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, SQLTypeId::BIGINT, val)) {
		return 0;
	}
	return val.value_.bigint;
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(pStmt, iCol, SQLTypeId::VARCHAR, val)) {
		return nullptr;
	}
	try {
		if (!pStmt->current_text) {
			pStmt->current_text =
			    unique_ptr<sqlite3_string_buffer[]>(new sqlite3_string_buffer[pStmt->result->sql_types.size()]);
		}
		auto &entry = pStmt->current_text[iCol];
		if (!entry.data) {
			// not initialized yet, convert the value and initialize it
			entry.data = unique_ptr<char[]>(new char[val.str_value.size() + 1]);
			memcpy(entry.data.get(), val.str_value.c_str(), val.str_value.size() + 1);
		}
		return (const unsigned char *)entry.data.get();
	} catch (...) {
		// memory error!
		return nullptr;
	}
}

////////////////////////////
//      sqlite3_bind      //
////////////////////////////
int sqlite3_bind_parameter_count(sqlite3_stmt *stmt) {
	if (!stmt) {
		return 0;
	}
	return stmt->prepared->n_param;
}

const char *sqlite3_bind_parameter_name(sqlite3_stmt *stmt, int idx) {
	if (!stmt) {
		return nullptr;
	}
	if (idx < 1 || idx > (int)stmt->prepared->n_param) {
		return nullptr;
	}
	return stmt->bound_names[idx - 1].c_str();
}

int sqlite3_bind_parameter_index(sqlite3_stmt *stmt, const char *zName) {
	if (!stmt || !zName) {
		return 0;
	}
	for (index_t i = 0; i < stmt->bound_names.size(); i++) {
		if (stmt->bound_names[i] == string(zName)) {
			return i + 1;
		}
	}
	return 0;
}

int sqlite3_internal_bind_value(sqlite3_stmt *stmt, int idx, Value value) {
	if (!stmt || !stmt->prepared || stmt->result) {
		return SQLITE_MISUSE;
	}
	if (idx < 1 || idx > (int)stmt->prepared->n_param) {
		return SQLITE_RANGE;
	}
	stmt->bound_values[idx - 1] = value;
	return SQLITE_OK;
}

int sqlite3_bind_int(sqlite3_stmt *stmt, int idx, int val) {
	return sqlite3_internal_bind_value(stmt, idx, Value::INTEGER(val));
}

int sqlite3_bind_int64(sqlite3_stmt *stmt, int idx, sqlite3_int64 val) {
	return sqlite3_internal_bind_value(stmt, idx, Value::BIGINT(val));
}

int sqlite3_bind_null(sqlite3_stmt *stmt, int idx) {
	return sqlite3_internal_bind_value(stmt, idx, Value());
}

int sqlite3_bind_text(sqlite3_stmt *stmt, int idx, const char *val, int length, void (*free_func)(void *)) {
	if (!val) {
		return SQLITE_MISUSE;
	}
	string value;
	if (length < 0) {
		value = string(val);
	} else {
		value = string(val, val + length);
	}
	if (free_func) {
		free_func((void *)val);
	}
	return sqlite3_internal_bind_value(stmt, idx, Value(value));
}

int sqlite3_clear_bindings(sqlite3_stmt *stmt) {
	if (!stmt) {
		return SQLITE_MISUSE;
	}
	return SQLITE_OK;
}

int sqlite3_initialize(void) {
	return SQLITE_OK;
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
	if (pStmt) {
		delete pStmt;
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

char *sqlite3_strdup(const char *str) {
	char *result = (char *)sqlite3_malloc64(strlen(str) + 1);
	strcpy(result, str);
	return result;
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

int sqlite3_errcode(sqlite3 *db) {
	return db->last_error.empty() ? SQLITE_OK : SQLITE_ERROR;
}

int sqlite3_extended_errcode(sqlite3 *db) {
	return sqlite3_errcode(db);
}

const char *sqlite3_errmsg(sqlite3 *db) {
	return db->last_error.c_str();
}

void sqlite3_interrupt(sqlite3 *db) {
	db->con->Interrupt();
}

const char *sqlite3_libversion(void) {
	return LIB_VERSION;
}
const char *sqlite3_sourceid(void) {
	return SOURCE_ID;
}

int sqlite3_reset(sqlite3_stmt *stmt) {
	if (stmt) {
		stmt->result = nullptr;
		stmt->current_chunk = nullptr;
	}
	return SQLITE_OK;
}
