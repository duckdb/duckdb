#ifdef USE_DUCKDB_SHELL_WRAPPER
#include "duckdb_shell_wrapper.h"
#endif
#include "cast_sqlite.hpp"
#include "duckdb.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "sqlite3.h"
#include "sqlite3_udf_wrapper.hpp"
#include "udf_struct_sqlite3.h"
#include "utf8proc_wrapper.hpp"
#ifdef SHELL_INLINE_AUTOCOMPLETE
#include "autocomplete_extension.hpp"
#endif
#include "shell_extension.hpp"

#include <cassert>
#include <chrono>
#include <climits>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <thread>
#include <time.h>

using namespace duckdb;
using namespace std;

extern "C" {
char *sqlite3_print_duckbox(sqlite3_stmt *pStmt, size_t max_rows, size_t max_width, char *null_value, int columnar);
}

static char *sqlite3_strdup(const char *str);

struct sqlite3_string_buffer {
	//! String data
	duckdb::unsafe_unique_array<char> data;
	//! String length
	int data_len;
};

struct sqlite3_stmt {
	//! The DB object that this statement belongs to
	sqlite3 *db;
	//! The query string
	string query_string;
	//! The prepared statement object, if successfully prepared
	duckdb::unique_ptr<PreparedStatement> prepared;
	//! The result object, if successfully executed
	duckdb::unique_ptr<QueryResult> result;
	//! The current chunk that we are iterating over
	duckdb::unique_ptr<DataChunk> current_chunk;
	//! The current row into the current chunk that we are iterating over
	int64_t current_row;
	//! Bound values, used for binding to the prepared statement
	duckdb::vector<Value> bound_values;
	//! Names of the prepared parameters
	duckdb::vector<string> bound_names;
	//! The current column values converted to string, used and filled by sqlite3_column_text
	duckdb::unique_ptr<sqlite3_string_buffer[]> current_text;
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
	return sqlite3_open_v2(filename, ppDb, 0, NULL);
}

int sqlite3_open_v2(const char *filename, /* Database filename (UTF-8) */
                    sqlite3 **ppDb,       /* OUT: SQLite db handle */
                    int flags,            /* Flags */
                    const char *zVfs      /* Name of VFS module to use */
) {
	if (filename && strcmp(filename, ":memory:") == 0) {
		filename = NULL;
	}
	*ppDb = nullptr;
	if (zVfs) { /* unsupported so if set we complain */
		return SQLITE_ERROR;
	}
	int rc = SQLITE_OK;
	sqlite3 *pDb = nullptr;
	try {
		pDb = new sqlite3();
		DBConfig config;
		config.SetOptionByName("duckdb_api", "cli");
		config.options.access_mode = AccessMode::AUTOMATIC;
		if (flags & SQLITE_OPEN_READONLY) {
			config.options.access_mode = AccessMode::READ_ONLY;
		}
		if (flags & DUCKDB_UNSIGNED_EXTENSIONS) {
			config.options.allow_unsigned_extensions = true;
		}
		if (flags & DUCKDB_UNREDACTED_SECRETS) {
			config.options.allow_unredacted_secrets = true;
		}

		config.error_manager->AddCustomError(
		    ErrorType::UNSIGNED_EXTENSION,
		    "Extension \"%s\" could not be loaded because its signature is either missing or invalid and unsigned "
		    "extensions are disabled by configuration.\nStart the shell with the -unsigned parameter to allow this "
		    "(e.g. duckdb -unsigned).");
		pDb->db = make_uniq<DuckDB>(filename, &config);
#ifdef SHELL_INLINE_AUTOCOMPLETE
		pDb->db->LoadStaticExtension<AutocompleteExtension>();
#endif
		pDb->db->LoadStaticExtension<ShellExtension>();
		pDb->con = make_uniq<Connection>(*pDb->db);
	} catch (const Exception &ex) {
		if (pDb) {
			pDb->last_error = ErrorData(ex);
			pDb->errCode = SQLITE_ERROR;
		}
		rc = SQLITE_ERROR;
	} catch (std::exception &ex) {
		if (pDb) {
			pDb->last_error = ErrorData(ex);
			pDb->errCode = SQLITE_ERROR;
		}
		rc = SQLITE_ERROR;
	}
	*ppDb = pDb;
	return rc;
}

int sqlite3_close(sqlite3 *db) {
	if (db) {
		delete db;
	}
	return SQLITE_OK;
}

int sqlite3_shutdown(void) {
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
		Parser parser(db->con->context->GetParserOptions());
		parser.ParseQuery(query);
		if (parser.statements.size() == 0) {
			return SQLITE_OK;
		}
		// extract the remainder
		idx_t next_location = parser.statements[0]->stmt_location + parser.statements[0]->stmt_length;
		bool set_remainder = next_location < query.size();

		// extract the first statement
		duckdb::vector<duckdb::unique_ptr<SQLStatement>> statements;
		statements.push_back(std::move(parser.statements[0]));

		db->con->context->HandlePragmaStatements(statements);
		if (statements.empty()) {
			return SQLITE_OK;
		}

		// if there are multiple statements here, we are dealing with an import database statement
		// we directly execute all statements besides the final one
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto res = db->con->Query(std::move(statements[i]));
			if (res->HasError()) {
				db->last_error = res->GetErrorObject();
				return SQLITE_ERROR;
			}
		}

		// now prepare the query
		auto prepared = db->con->Prepare(std::move(statements.back()));
		if (prepared->HasError()) {
			// failed to prepare: set the error message
			db->last_error = prepared->error;
			return SQLITE_ERROR;
		}

		// create the statement entry
		duckdb::unique_ptr<sqlite3_stmt> stmt = make_uniq<sqlite3_stmt>();
		stmt->db = db;
		stmt->query_string = query;
		stmt->prepared = std::move(prepared);
		stmt->current_row = -1;
		for (idx_t i = 0; i < stmt->prepared->named_param_map.size(); i++) {
			stmt->bound_names.push_back("$" + to_string(i + 1));
			stmt->bound_values.push_back(Value());
		}

		// extract the remainder of the query and assign it to the pzTail
		if (pzTail && set_remainder) {
			*pzTail = zSql + next_location + 1;
		}

		*ppStmt = stmt.release();
		return SQLITE_OK;
	} catch (std::exception &ex) {
		db->last_error = ErrorData(ex);
		db->con->context->ProcessError(db->last_error, query);
		return SQLITE_ERROR;
	}
}

char *sqlite3_print_duckbox(sqlite3_stmt *pStmt, size_t max_rows, size_t max_width, char *null_value, int columnar) {
	try {
		if (!pStmt) {
			return nullptr;
		}
		if (!pStmt->prepared) {
			pStmt->db->last_error = ErrorData("Attempting sqlite3_step() on a non-successfully prepared statement");
			return nullptr;
		}
		if (pStmt->result) {
			pStmt->db->last_error = ErrorData("Statement has already been executed");
			return nullptr;
		}
		pStmt->result = pStmt->prepared->Execute(pStmt->bound_values, false);
		if (pStmt->result->HasError()) {
			// error in execute: clear prepared statement
			pStmt->db->last_error = pStmt->result->GetErrorObject();
			pStmt->prepared = nullptr;
			return nullptr;
		}
		auto &materialized = (MaterializedQueryResult &)*pStmt->result;
		auto properties = pStmt->prepared->GetStatementProperties();
		if (properties.return_type == StatementReturnType::CHANGED_ROWS && materialized.RowCount() > 0) {
			// update total changes
			auto row_changes = materialized.Collection().GetRows().GetValue(0, 0);
			if (!row_changes.IsNull() && row_changes.DefaultTryCastAs(LogicalType::BIGINT)) {
				pStmt->db->last_changes = row_changes.GetValue<int64_t>();
				pStmt->db->total_changes += row_changes.GetValue<int64_t>();
			}
		}
		if (properties.return_type != StatementReturnType::QUERY_RESULT) {
			// only SELECT statements return results
			return nullptr;
		}
		BoxRendererConfig config;
		if (max_rows != 0) {
			config.max_rows = max_rows;
		}
		if (null_value) {
			config.null_value = null_value;
		}
		if (columnar) {
			config.render_mode = RenderMode::COLUMNS;
		}
		config.max_width = max_width;
		BoxRenderer renderer(config);
		auto result_rendering =
		    renderer.ToString(*pStmt->db->con->context, pStmt->result->names, materialized.Collection());
		return sqlite3_strdup(result_rendering.c_str());
	} catch (std::exception &ex) {
		string error_str = ErrorData(ex).Message() + "\n";
		return sqlite3_strdup(error_str.c_str());
	}
}

/* Prepare the next result to be retrieved */
int sqlite3_step(sqlite3_stmt *pStmt) {
	if (!pStmt) {
		return SQLITE_MISUSE;
	}
	if (!pStmt->prepared) {
		pStmt->db->last_error = ErrorData("Attempting sqlite3_step() on a non-successfully prepared statement");
		return SQLITE_ERROR;
	}
	pStmt->current_text = nullptr;
	if (!pStmt->result) {
		// no result yet! call Execute()
		pStmt->result = pStmt->prepared->Execute(pStmt->bound_values, true);
		if (pStmt->result->HasError()) {
			// error in execute: clear prepared statement
			pStmt->db->last_error = pStmt->result->GetErrorObject();
			pStmt->prepared = nullptr;
			return SQLITE_ERROR;
		}
		// fetch a chunk
		if (!pStmt->result->TryFetch(pStmt->current_chunk, pStmt->db->last_error)) {
			pStmt->prepared = nullptr;
			return SQLITE_ERROR;
		}

		pStmt->current_row = -1;

		auto properties = pStmt->prepared->GetStatementProperties();
		if (properties.return_type == StatementReturnType::CHANGED_ROWS && pStmt->current_chunk &&
		    pStmt->current_chunk->size() > 0) {
			// update total changes
			auto row_changes = pStmt->current_chunk->GetValue(0, 0);
			if (!row_changes.IsNull() && row_changes.DefaultTryCastAs(LogicalType::BIGINT)) {
				pStmt->db->last_changes = row_changes.GetValue<int64_t>();
				pStmt->db->total_changes += row_changes.GetValue<int64_t>();
			}
		}
		if (properties.return_type != StatementReturnType::QUERY_RESULT) {
			// only SELECT statements return results
			sqlite3_reset(pStmt);
		}
	}
	if (!pStmt->current_chunk || pStmt->current_chunk->size() == 0) {
		return SQLITE_DONE;
	}
	pStmt->current_row++;
	if (pStmt->current_row >= (int32_t)pStmt->current_chunk->size()) {
		// have to fetch again!
		pStmt->current_row = 0;
		if (!pStmt->result->TryFetch(pStmt->current_chunk, pStmt->db->last_error)) {
			pStmt->prepared = nullptr;
			return SQLITE_ERROR;
		}
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
	if (!pStmt || !pStmt->prepared) {
		return 0;
	}
	return (int)pStmt->prepared->ColumnCount();
}

////////////////////////////
//     sqlite3_column     //
////////////////////////////
int sqlite3_column_type(sqlite3_stmt *pStmt, int iCol) {
	if (!pStmt || !pStmt->result || !pStmt->current_chunk) {
		return 0;
	}
	if (FlatVector::IsNull(pStmt->current_chunk->data[iCol], pStmt->current_row)) {
		return SQLITE_NULL;
	}
	auto column_type = pStmt->result->types[iCol];
	if (column_type.IsJSONType()) {
		return 0; // Does not need to be surrounded in quotes like VARCHAR
	}
	if (column_type.HasAlias()) {
		// Use the text representation for aliased types
		return SQLITE_TEXT;
	}
	switch (column_type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT: /* TODO: Maybe blob? */
		return SQLITE_INTEGER;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return SQLITE_FLOAT;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::LIST:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		return SQLITE_TEXT;
	case LogicalTypeId::BLOB:
		return SQLITE_BLOB;
	default:
		// TODO(wangfenjin): agg function don't have type?
		return SQLITE_TEXT;
	}
	return 0;
}

const char *sqlite3_column_name(sqlite3_stmt *pStmt, int N) {
	if (!pStmt || !pStmt->prepared) {
		return nullptr;
	}
	return pStmt->prepared->GetNames()[N].c_str();
}

static bool sqlite3_column_has_value(sqlite3_stmt *pStmt, int iCol, LogicalType target_type, Value &val) {
	if (!pStmt || !pStmt->result || !pStmt->current_chunk) {
		return false;
	}
	if (iCol < 0 || iCol >= (int)pStmt->result->types.size()) {
		return false;
	}
	if (FlatVector::IsNull(pStmt->current_chunk->data[iCol], pStmt->current_row)) {
		return false;
	}
	try {
		val =
		    pStmt->current_chunk->data[iCol].GetValue(pStmt->current_row).CastAs(*pStmt->db->con->context, target_type);
	} catch (...) {
		return false;
	}
	return true;
}

double sqlite3_column_double(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, LogicalType::DOUBLE, val)) {
		return 0;
	}
	return DoubleValue::Get(val);
}

int sqlite3_column_int(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, LogicalType::INTEGER, val)) {
		return 0;
	}
	return IntegerValue::Get(val);
}

sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *stmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(stmt, iCol, LogicalType::BIGINT, val)) {
		return 0;
	}
	return BigIntValue::Get(val);
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(pStmt, iCol, LogicalType::VARCHAR, val)) {
		return nullptr;
	}
	try {
		if (!pStmt->current_text) {
			pStmt->current_text =
			    duckdb::unique_ptr<sqlite3_string_buffer[]>(new sqlite3_string_buffer[pStmt->result->types.size()]);
		}
		auto &entry = pStmt->current_text[iCol];
		if (!entry.data) {
			// not initialized yet, convert the value and initialize it
			auto &str_val = StringValue::Get(val);
			entry.data = duckdb::make_unsafe_uniq_array<char>(str_val.size() + 1);
			memcpy(entry.data.get(), str_val.c_str(), str_val.size() + 1);
			entry.data_len = str_val.length();
		}
		return (const unsigned char *)entry.data.get();
	} catch (...) {
		// memory error!
		return nullptr;
	}
}

const void *sqlite3_column_blob(sqlite3_stmt *pStmt, int iCol) {
	Value val;
	if (!sqlite3_column_has_value(pStmt, iCol, LogicalType::BLOB, val)) {
		return nullptr;
	}
	try {
		if (!pStmt->current_text) {
			pStmt->current_text =
			    duckdb::unique_ptr<sqlite3_string_buffer[]>(new sqlite3_string_buffer[pStmt->result->types.size()]);
		}
		auto &entry = pStmt->current_text[iCol];
		if (!entry.data) {
			// not initialized yet, convert the value and initialize it
			auto &str_val = StringValue::Get(val);
			entry.data = duckdb::make_unsafe_uniq_array<char>(str_val.size() + 1);
			memcpy(entry.data.get(), str_val.c_str(), str_val.size() + 1);
			entry.data_len = str_val.length();
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
	return stmt->prepared->named_param_map.size();
}

const char *sqlite3_bind_parameter_name(sqlite3_stmt *stmt, int idx) {
	if (!stmt) {
		return nullptr;
	}
	if (idx < 1 || idx > (int)stmt->prepared->named_param_map.size()) {
		return nullptr;
	}
	return stmt->bound_names[idx - 1].c_str();
}

int sqlite3_bind_parameter_index(sqlite3_stmt *stmt, const char *zName) {
	if (!stmt || !zName) {
		return 0;
	}
	for (idx_t i = 0; i < stmt->bound_names.size(); i++) {
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
	if (idx < 1 || idx > (int)stmt->prepared->named_param_map.size()) {
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

int sqlite3_bind_double(sqlite3_stmt *stmt, int idx, double val) {
	return sqlite3_internal_bind_value(stmt, idx, Value::DOUBLE(val));
}

int sqlite3_bind_null(sqlite3_stmt *stmt, int idx) {
	return sqlite3_internal_bind_value(stmt, idx, Value());
}

SQLITE_API int sqlite3_bind_value(sqlite3_stmt *, int, const sqlite3_value *) {
	fprintf(stderr, "sqlite3_bind_value: unsupported.\n");
	return SQLITE_ERROR;
}

int sqlite3_bind_text(sqlite3_stmt *stmt, int idx, const char *val, int length, void (*free_func)(void *)) {
	if (!val) {
		return SQLITE_MISUSE;
	}
	string value;
	if (length < 0) {
		value = string(val);
	} else {
		value = string(val, length);
	}
	if (free_func && ((ptrdiff_t)free_func) != -1) {
		free_func((void *)val);
		val = nullptr;
	}
	try {
		return sqlite3_internal_bind_value(stmt, idx, Value(value));
	} catch (std::exception &ex) {
		return SQLITE_ERROR;
	}
}

int sqlite3_bind_blob(sqlite3_stmt *stmt, int idx, const void *val, int length, void (*free_func)(void *)) {
	if (!val) {
		return SQLITE_MISUSE;
	}
	Value blob;
	if (length < 0) {
		blob = Value::BLOB(string((const char *)val));
	} else {
		blob = Value::BLOB(const_data_ptr_cast(val), length);
	}
	if (free_func && ((ptrdiff_t)free_func) != -1) {
		free_func((void *)val);
		val = nullptr;
	}
	try {
		return sqlite3_internal_bind_value(stmt, idx, blob);
	} catch (std::exception &ex) {
		return SQLITE_ERROR;
	}
}

SQLITE_API int sqlite3_bind_zeroblob(sqlite3_stmt *stmt, int idx, int length) {
	fprintf(stderr, "sqlite3_bind_zeroblob: unsupported.\n");
	return SQLITE_ERROR;
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
		if (pStmt->result && pStmt->result->HasError()) {
			pStmt->db->last_error = pStmt->result->GetErrorObject();
			delete pStmt;
			return SQLITE_ERROR;
		}

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

void *sqlite3_malloc(int n) {
	return sqlite3_malloc64(n);
}

void *sqlite3_realloc(void *ptr, int n) {
	return sqlite3_realloc64(ptr, n);
}

void *sqlite3_realloc64(void *ptr, sqlite3_uint64 n) {
	return realloc(ptr, n);
}

// TODO: stub
int sqlite3_config(int i, ...) {
	return SQLITE_OK;
}

int sqlite3_errcode(sqlite3 *db) {
	if (!db) {
		return SQLITE_NOMEM;
	}
	// return db->last_error.empty() ? SQLITE_OK : SQLITE_ERROR;
	return db->errCode; //! We should return the exact error code
}

int sqlite3_extended_errcode(sqlite3 *db) {
	return sqlite3_errcode(db);
}

const char *sqlite3_errmsg(sqlite3 *db) {
	if (!db) {
		return "";
	}
	return db->last_error.Message().c_str();
}

void sqlite3_interrupt(sqlite3 *db) {
	if (db && db->con) {
		db->con->Interrupt();
	}
}

const char *sqlite3_libversion(void) {
	return DuckDB::LibraryVersion();
}

const char *sqlite3_sourceid(void) {
	return DuckDB::SourceID();
}

int sqlite3_reset(sqlite3_stmt *stmt) {
	if (stmt) {
		stmt->result = nullptr;
		stmt->current_chunk = nullptr;
	}
	return SQLITE_OK;
}

// support functions for shell.c
// most are dummies, we don't need them really

int sqlite3_db_status(sqlite3 *, int op, int *pCur, int *pHiwtr, int resetFlg) {
	fprintf(stderr, "sqlite3_db_status: unsupported.\n");
	return -1;
}

int sqlite3_changes(sqlite3 *db) {
	return db->last_changes;
}

sqlite3_int64 sqlite3_changes64(sqlite3 *db) {
	return db->last_changes;
}

int sqlite3_total_changes(sqlite3 *db) {
	return db->total_changes;
}

sqlite3_int64 sqlite3_total_changes64(sqlite3 *db) {
	return db->total_changes;
}

SQLITE_API sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db) {
	return SQLITE_ERROR;
}

enum class SQLParseState { SEMICOLON, WHITESPACE, NORMAL };

const char *skipDollarQuotedString(const char *zSql, const char *delimiterStart, idx_t delimiterLength) {
	for (; *zSql; zSql++) {
		if (*zSql == '$') {
			// found a dollar
			// move forward and find the next dollar
			zSql++;
			auto start = zSql;
			while (*zSql && *zSql != '$') {
				zSql++;
			}
			if (!zSql[0]) {
				// reached end of string while looking for the dollar
				return nullptr;
			}
			// check if the dollar quoted string name matches
			if (delimiterLength == idx_t(zSql - start)) {
				if (memcmp(start, delimiterStart, delimiterLength) == 0) {
					return zSql;
				}
			}
			// dollar does not match - reset position to start and keep looking
			zSql = start - 1;
		}
	}
	// unterminated
	return nullptr;
}

int sqlite3_complete(const char *zSql) {
	auto state = SQLParseState::NORMAL;

	for (; *zSql; zSql++) {
		SQLParseState next_state;
		switch (*zSql) {
		case ';':
			next_state = SQLParseState::SEMICOLON;
			break;
		case ' ':
		case '\r':
		case '\t':
		case '\n':
		case '\f': { /* White space is ignored */
			next_state = SQLParseState::WHITESPACE;
			break;
		}
		case '/': { /* C-style comments */
			if (zSql[1] != '*') {
				next_state = SQLParseState::NORMAL;
				break;
			}
			zSql += 2;
			while (zSql[0] && (zSql[0] != '*' || zSql[1] != '/')) {
				zSql++;
			}
			if (zSql[0] == 0) {
				// unterminated c-style string
				return 0;
			}
			zSql++;
			next_state = SQLParseState::WHITESPACE;
			break;
		}
		case '-': { /* SQL-style comments from "--" to end of line */
			if (zSql[1] != '-') {
				next_state = SQLParseState::NORMAL;
				break;
			}
			while (*zSql && *zSql != '\n') {
				zSql++;
			}
			if (*zSql == 0) {
				// unterminated SQL-style comment - return whether or not we had a semicolon right before it
				return state == SQLParseState::SEMICOLON ? 1 : 0;
			}
			next_state = SQLParseState::WHITESPACE;
			break;
		}
		case '$': { /* Dollar-quoted strings */
			// check if this is a dollar-quoted string
			idx_t next_dollar = 0;
			for (idx_t idx = 1; zSql[idx]; idx++) {
				if (zSql[idx] == '$') {
					// found the next dollar
					next_dollar = idx;
					break;
				}
				// all characters can be between A-Z, a-z or \200 - \377
				if (zSql[idx] >= 'A' && zSql[idx] <= 'Z') {
					continue;
				}
				if (zSql[idx] >= 'a' && zSql[idx] <= 'z') {
					continue;
				}
				if (zSql[idx] >= '\200' && zSql[idx] <= '\377') {
					continue;
				}
				// the first character CANNOT be a numeric, only subsequent characters
				if (idx > 1 && zSql[idx] >= '0' && zSql[idx] <= '9') {
					continue;
				}
				// not a dollar quoted string
				break;
			}
			if (next_dollar == 0) {
				// not a dollar quoted string
				next_state = SQLParseState::NORMAL;
				break;
			}
			auto start = zSql + 1;
			zSql += next_dollar;
			const char *delimiterStart = start;
			idx_t delimiterLength = zSql - start;
			zSql++;
			// skip the dollar quoted string
			zSql = skipDollarQuotedString(zSql, delimiterStart, delimiterLength);
			if (!zSql) {
				// unterminated dollar string
				return 0;
			}
			next_state = SQLParseState::WHITESPACE;
			break;
		}
			//		case '`': /* Grave-accent quoted symbols used by MySQL */
		case '"': /* single- and double-quoted strings */
		case '\'': {
			int c = *zSql;
			zSql++;
			while (*zSql && *zSql != c) {
				zSql++;
			}
			if (*zSql == 0) {
				// unterminated single or double quoted string
				return 0;
			}
			next_state = SQLParseState::WHITESPACE;
			break;
		}
		default:
			next_state = SQLParseState::NORMAL;
		}
		// white space is ignored (no change in state)
		if (next_state != SQLParseState::WHITESPACE) {
			state = next_state;
		}
	}
	return state == SQLParseState::SEMICOLON ? 1 : 0;
}

// length of varchar or blob value
int sqlite3_column_bytes(sqlite3_stmt *pStmt, int iCol) {

	if (!pStmt || iCol < 0 || pStmt->result->types.size() <= static_cast<size_t>(iCol))
		return 0;

	// checks if the current column is initialized
	if (!pStmt->current_text) {
		if (!sqlite3_column_text(pStmt, iCol) && !sqlite3_column_blob(pStmt, iCol)) {
			return 0;
		}
	}
	sqlite3_string_buffer *col_text = &pStmt->current_text[iCol];
	if (!col_text->data) {
		if (!sqlite3_column_text(pStmt, iCol) && !sqlite3_column_blob(pStmt, iCol)) {
			return 0;
		}
		col_text = &pStmt->current_text[iCol];
	}

	return col_text->data_len;
}

sqlite3_value *sqlite3_column_value(sqlite3_stmt *, int iCol) {
	fprintf(stderr, "sqlite3_column_value: unsupported.\n");
	return nullptr;
}

int sqlite3_db_config(sqlite3 *, int op, ...) {
	fprintf(stderr, "sqlite3_db_config: unsupported.\n");
	return -1;
}

int sqlite3_get_autocommit(sqlite3 *db) {
	return db->con->context->transaction.IsAutoCommit();
}

int sqlite3_limit(sqlite3 *, int id, int newVal) {
	if (newVal >= 0) {
		// attempting to set limit value
		return SQLITE_OK;
	}
	switch (id) {
	case SQLITE_LIMIT_LENGTH:
	case SQLITE_LIMIT_SQL_LENGTH:
	case SQLITE_LIMIT_COLUMN:
	case SQLITE_LIMIT_LIKE_PATTERN_LENGTH:
		return std::numeric_limits<int>::max();
	case SQLITE_LIMIT_EXPR_DEPTH:
		return 1000;
	case SQLITE_LIMIT_FUNCTION_ARG:
	case SQLITE_LIMIT_VARIABLE_NUMBER:
		return 256;
	case SQLITE_LIMIT_ATTACHED:
		return 1000;
	case SQLITE_LIMIT_WORKER_THREADS:
	case SQLITE_LIMIT_TRIGGER_DEPTH:
		return 0;
	default:
		return SQLITE_ERROR;
	}
}

int sqlite3_stmt_readonly(sqlite3_stmt *pStmt) {
	fprintf(stderr, "sqlite3_stmt_readonly: unsupported.\n");
	return -1;
}

// TODO pretty easy schema lookup
int sqlite3_table_column_metadata(sqlite3 *db,             /* Connection handle */
                                  const char *zDbName,     /* Database name or NULL */
                                  const char *zTableName,  /* Table name */
                                  const char *zColumnName, /* Column name */
                                  char const **pzDataType, /* OUTPUT: Declared data type */
                                  char const **pzCollSeq,  /* OUTPUT: Collation sequence name */
                                  int *pNotNull,           /* OUTPUT: True if NOT NULL constraint exists */
                                  int *pPrimaryKey,        /* OUTPUT: True if column part of PK */
                                  int *pAutoinc            /* OUTPUT: True if column is auto-increment */
) {
	fprintf(stderr, "sqlite3_table_column_metadata: unsupported.\n");
	return -1;
}

const char *sqlite3_column_table_name(sqlite3_stmt *pStmt, int iCol) {
	if (!pStmt || !pStmt->prepared) {
		return nullptr;
	}

	auto &&names = pStmt->prepared->GetNames();
	if (iCol < 0 || names.size() <= static_cast<size_t>(iCol)) {
		return nullptr;
	}

	return names[iCol].c_str();
}

const char *sqlite3_column_decltype(sqlite3_stmt *pStmt, int iCol) {
	if (!pStmt || !pStmt->prepared) {
		return nullptr;
	}

	auto &&types = pStmt->prepared->GetTypes();
	if (iCol < 0 || types.size() <= static_cast<size_t>(iCol)) {
		return nullptr;
	}

	auto column_type = types[iCol];
	switch (column_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::BLOB:
		return "BLOB";
	default:
		return NULL;
	}
	return NULL;
}

SQLITE_API int sqlite3_status64(int op, sqlite3_int64 *pCurrent, sqlite3_int64 *pHighwater, int resetFlag) {
	fprintf(stderr, "sqlite3_status64: unsupported.\n");
	return -1;
}

int sqlite3_stmt_status(sqlite3_stmt *, int op, int resetFlg) {
	fprintf(stderr, "sqlite3_stmt_status: unsupported.\n");
	return -1;
}

int sqlite3_file_control(sqlite3 *, const char *zDbName, int op, void *ptr) {
	switch (op) {
	case SQLITE_FCNTL_TEMPFILENAME: {
		auto char_arg = (char **)ptr;
		*char_arg = nullptr;
		return -1;
	}
	default:
		break;
	}
	fprintf(stderr, "sqlite3_file_control op %d: unsupported.\n", op);
	return -1;
}

int sqlite3_declare_vtab(sqlite3 *, const char *zSQL) {
	fprintf(stderr, "sqlite3_declare_vtab: unsupported.\n");
	return -1;
}

const char *sqlite3_vtab_collation(sqlite3_index_info *, int) {
	fprintf(stderr, "sqlite3_vtab_collation: unsupported.\n");
	return nullptr;
}

int sqlite3_sleep(int ms) {
	std::this_thread::sleep_for(std::chrono::milliseconds(ms));
	return ms;
}

int sqlite3_busy_timeout(sqlite3 *, int ms) {
	fprintf(stderr, "sqlite3_busy_timeout: unsupported.\n");
	return -1;
}

// unlikely to be supported

int sqlite3_trace_v2(sqlite3 *, unsigned uMask, int (*xCallback)(unsigned, void *, void *, void *), void *pCtx) {
	fprintf(stderr, "sqlite3_trace_v2: unsupported.\n");
	return -1;
}

int sqlite3_test_control(int op, ...) {
	fprintf(stderr, "sqlite3_test_control: unsupported.\n");
	return -1;
}

int sqlite3_enable_load_extension(sqlite3 *db, int onoff) {
	// fprintf(stderr, "sqlite3_enable_load_extension: unsupported.\n");
	return -1;
}

int sqlite3_load_extension(sqlite3 *db,       /* Load the extension into this database connection */
                           const char *zFile, /* Name of the shared library containing extension */
                           const char *zProc, /* Entry point.  Derived from zFile if 0 */
                           char **pzErrMsg    /* Put error message here if not 0 */
) {
	// fprintf(stderr, "sqlite3_load_extension: unsupported.\n");
	return -1;
}

int sqlite3_create_module(sqlite3 *db,             /* SQLite connection to register module with */
                          const char *zName,       /* Name of the module */
                          const sqlite3_module *p, /* Methods for the module */
                          void *pClientData        /* Client data for xCreate/xConnect */
) {
	// fprintf(stderr, "sqlite3_create_module: unsupported.\n");
	return -1;
}

int sqlite3_create_function(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp,
                            void (*xFunc)(sqlite3_context *, int, sqlite3_value **),
                            void (*xStep)(sqlite3_context *, int, sqlite3_value **),
                            void (*xFinal)(sqlite3_context *)) {
	if ((!xFunc && !xStep && !xFinal) || !zFunctionName || nArg < -1) {
		return SQLITE_MISUSE;
	}
	string fname = string(zFunctionName);

	// Scalar function
	if (!xFunc) {
		return SQLITE_MISUSE;
	}
	auto udf_sqlite3 = SQLiteUDFWrapper::CreateSQLiteScalarFunction(xFunc, db, pApp);
	LogicalType varargs = LogicalType::INVALID;
	if (nArg == -1) {
		varargs = LogicalType::ANY;
		nArg = 0;
	}

	duckdb::vector<LogicalType> argv_types(nArg);
	for (idx_t i = 0; i < (idx_t)nArg; ++i) {
		argv_types[i] = LogicalType::ANY;
	}

	UDFWrapper::RegisterFunction(fname, argv_types, LogicalType::VARCHAR, udf_sqlite3, *(db->con->context), varargs);
	return SQLITE_OK;
}

int sqlite3_create_function_v2(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp,
                               void (*xFunc)(sqlite3_context *, int, sqlite3_value **),
                               void (*xStep)(sqlite3_context *, int, sqlite3_value **),
                               void (*xFinal)(sqlite3_context *), void (*xDestroy)(void *)) {
	return -1;
}

int sqlite3_set_authorizer(sqlite3 *, int (*xAuth)(void *, int, const char *, const char *, const char *, const char *),
                           void *pUserData) {
	fprintf(stderr, "sqlite3_set_authorizer: unsupported.\n");
	return -1;
}

// needed in shell timer
static int unixCurrentTimeInt64(sqlite3_vfs *NotUsed, sqlite3_int64 *piNow) {
	using namespace std::chrono;
	*piNow = (sqlite3_int64)duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
	return SQLITE_OK;
}

static sqlite3_vfs static_sqlite3_virtual_file_systems[] = {{
    3,                    // int iVersion;            /* Structure version number (currently 3) */
    0,                    // int szOsFile;            /* Size of subclassed sqlite3_file */
    0,                    // int mxPathname;          /* Maximum file pathname length */
    nullptr,              // sqlite3_vfs *pNext;      /* Next registered VFS */
    "dummy",              // const char *zName;       /* Name of this virtual file system */
    nullptr,              // void *pAppData;          /* Pointer to application-specific data */
    nullptr,              // int (*xOpen)(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
    nullptr,              // int (*xDelete)(sqlite3_vfs*, const char *zName, int syncDir);
    nullptr,              // int (*xAccess)(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
    nullptr,              // int (*xFullPathname)(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
    nullptr,              // void *(*xDlOpen)(sqlite3_vfs*, const char *zFilename);
    nullptr,              // void (*xDlError)(sqlite3_vfs*, int nByte, char *zErrMsg);
    nullptr,              // void (*(*xDlSym)(sqlite3_vfs*,void*, const char *zSymbol))(void);
    nullptr,              // void (*xDlClose)(sqlite3_vfs*, void*);
    nullptr,              // int (*xRandomness)(sqlite3_vfs*, int nByte, char *zOut);
    nullptr,              // int (*xSleep)(sqlite3_vfs*, int microseconds);
    nullptr,              // int (*xCurrentTime)(sqlite3_vfs*, double*);
    nullptr,              // int (*xGetLastError)(sqlite3_vfs*, int, char *);
    unixCurrentTimeInt64, // int (*xCurrentTimeInt64)(sqlite3_vfs*, sqlite3_int64*);
    nullptr,              // int (*xSetSystemCall)(sqlite3_vfs*, const char *zName, sqlite3_syscall_ptr);
    nullptr,              // sqlite3_syscall_ptr (*xGetSystemCall)(sqlite3_vfs*, const char *zName);
    nullptr               // const char *(*xNextSystemCall)(sqlite3_vfs*, const char *zName);
}};

// virtual file system, providing some dummies to avoid crashes
sqlite3_vfs *sqlite3_vfs_find(const char *zVfsName) {
	// return a dummy because the shell does not check the return code.
	return static_sqlite3_virtual_file_systems;
}

int sqlite3_vfs_register(sqlite3_vfs *, int makeDflt) {
	// fprintf(stderr, "sqlite3_vfs_register: unsupported.\n");
	return -1;
}

// backups, unused

int sqlite3_backup_step(sqlite3_backup *p, int nPage) {
	fprintf(stderr, "sqlite3_backup_step: unsupported.\n");
	return -1;
}

int sqlite3_backup_finish(sqlite3_backup *p) {
	fprintf(stderr, "sqlite3_backup_finish: unsupported.\n");
	return -1;
}

sqlite3_backup *sqlite3_backup_init(sqlite3 *pDest,         /* Destination database handle */
                                    const char *zDestName,  /* Destination database name */
                                    sqlite3 *pSource,       /* Source database handle */
                                    const char *zSourceName /* Source database name */
) {
	fprintf(stderr, "sqlite3_backup_init: unsupported.\n");
	return nullptr;
}

// UDF support stuff, unused for now. These cannot be called as create_function above is disabled

SQLITE_API sqlite3 *sqlite3_context_db_handle(sqlite3_context *) {
	return nullptr;
}

void *sqlite3_user_data(sqlite3_context *context) {
	assert(context);
	return context->pFunc.pUserData;
}

#ifdef _WIN32
#include <windows.h>

static void *sqlite3MallocZero(size_t n) {
	auto res = sqlite3_malloc(n);
	assert(res);
	memset(res, 0, n);
	return res;
}

static LPWSTR winUtf8ToUnicode(const char *zText) {
	int nChar;
	LPWSTR zWideText;

	nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, NULL, 0);
	if (nChar == 0) {
		return 0;
	}
	zWideText = (LPWSTR)sqlite3MallocZero(nChar * sizeof(WCHAR));
	if (zWideText == 0) {
		return 0;
	}
	nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, zWideText, nChar);
	if (nChar == 0) {
		sqlite3_free(zWideText);
		zWideText = 0;
	}
	return zWideText;
}

static char *winUnicodeToMbcs(LPCWSTR zWideText, int useAnsi) {
	int nByte;
	char *zText;
	int codepage = useAnsi ? CP_ACP : CP_OEMCP;

	nByte = WideCharToMultiByte(codepage, 0, zWideText, -1, 0, 0, 0, 0);
	if (nByte == 0) {
		return 0;
	}
	zText = (char *)sqlite3MallocZero(nByte);
	if (zText == 0) {
		return 0;
	}
	nByte = WideCharToMultiByte(codepage, 0, zWideText, -1, zText, nByte, 0, 0);
	if (nByte == 0) {
		sqlite3_free(zText);
		zText = 0;
	}
	return zText;
}

static char *winUtf8ToMbcs(const char *zText, int useAnsi) {
	char *zTextMbcs;
	LPWSTR zTmpWide;

	zTmpWide = winUtf8ToUnicode(zText);
	if (zTmpWide == 0) {
		return 0;
	}
	zTextMbcs = winUnicodeToMbcs(zTmpWide, useAnsi);
	sqlite3_free(zTmpWide);
	return zTextMbcs;
}

SQLITE_API char *sqlite3_win32_utf8_to_mbcs_v2(const char *zText, int useAnsi) {
	return winUtf8ToMbcs(zText, useAnsi);
}

LPWSTR sqlite3_win32_utf8_to_unicode(const char *zText) {
	return winUtf8ToUnicode(zText);
}

static LPWSTR winMbcsToUnicode(const char *zText, int useAnsi) {
	int nByte;
	LPWSTR zMbcsText;
	int codepage = useAnsi ? CP_ACP : CP_OEMCP;

	nByte = MultiByteToWideChar(codepage, 0, zText, -1, NULL, 0) * sizeof(WCHAR);
	if (nByte == 0) {
		return 0;
	}
	zMbcsText = (LPWSTR)sqlite3MallocZero(nByte * sizeof(WCHAR));
	if (zMbcsText == 0) {
		return 0;
	}
	nByte = MultiByteToWideChar(codepage, 0, zText, -1, zMbcsText, nByte);
	if (nByte == 0) {
		sqlite3_free(zMbcsText);
		zMbcsText = 0;
	}
	return zMbcsText;
}

static char *winUnicodeToUtf8(LPCWSTR zWideText) {
	int nByte;
	char *zText;

	nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, 0, 0, 0, 0);
	if (nByte == 0) {
		return 0;
	}
	zText = (char *)sqlite3MallocZero(nByte);
	if (zText == 0) {
		return 0;
	}
	nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, zText, nByte, 0, 0);
	if (nByte == 0) {
		sqlite3_free(zText);
		zText = 0;
	}
	return zText;
}

static char *winMbcsToUtf8(const char *zText, int useAnsi) {
	char *zTextUtf8;
	LPWSTR zTmpWide;

	zTmpWide = winMbcsToUnicode(zText, useAnsi);
	if (zTmpWide == 0) {
		return 0;
	}
	zTextUtf8 = winUnicodeToUtf8(zTmpWide);
	sqlite3_free(zTmpWide);
	return zTextUtf8;
}

SQLITE_API char *sqlite3_win32_mbcs_to_utf8_v2(const char *zText, int useAnsi) {
	return winMbcsToUtf8(zText, useAnsi);
}

SQLITE_API char *sqlite3_win32_unicode_to_utf8(LPCWSTR zWideText) {
	return winUnicodeToUtf8(zWideText);
}

#endif

// TODO complain
SQLITE_API void sqlite3_result_blob(sqlite3_context *context, const void *blob, int n_bytes, void (*xDel)(void *)) {
	sqlite3_result_blob64(context, blob, n_bytes, xDel);
}

SQLITE_API void sqlite3_result_blob64(sqlite3_context *context, const void *blob, sqlite3_uint64 n_bytes,
                                      void (*xDel)(void *)) {
	if (!blob) {
		context->isError = SQLITE_MISUSE;
		return;
	}
	context->result.type = SQLiteTypeValue::BLOB;
	context->result.str = string((char *)blob, n_bytes);
	if (xDel && xDel != SQLITE_TRANSIENT) {
		xDel((void *)blob);
	}
}

SQLITE_API void sqlite3_result_double(sqlite3_context *context, double val) {
	context->result.u.r = val;
	context->result.type = SQLiteTypeValue::FLOAT;
}

SQLITE_API void sqlite3_result_error(sqlite3_context *context, const char *msg, int n_bytes) {
	context->isError = SQLITE_ERROR;
	sqlite3_result_text(context, msg, n_bytes, nullptr);
}

SQLITE_API void sqlite3_result_error16(sqlite3_context *context, const void *msg, int n_bytes) {
	fprintf(stderr, "sqlite3_result_error16: unsupported.\n");
}

SQLITE_API void sqlite3_result_error_toobig(sqlite3_context *context) {
	sqlite3_result_error(context, "string or blob too big", -1);
}

SQLITE_API void sqlite3_result_error_nomem(sqlite3_context *context) {
	sqlite3_result_error(context, "out of memory", -1);
}

SQLITE_API void sqlite3_result_error_code(sqlite3_context *context, int code) {
	string error_msg;
	switch (code) {
	case SQLITE_NOMEM:
		sqlite3_result_error_nomem(context);
		return;
	case SQLITE_TOOBIG:
		sqlite3_result_error_toobig(context);
		return;
	case SQLITE_ERROR:
		error_msg = "Generic error";
		break;
	case SQLITE_INTERNAL:
		error_msg = "Internal logic error in SQLite";
		break;
	case SQLITE_PERM:
		error_msg = "Access permission denied";
		break;
	case SQLITE_ABORT:
		error_msg = "Callback routine requested an abort";
		break;
	case SQLITE_BUSY:
		error_msg = "The database file is locked";
		break;
	case SQLITE_LOCKED:
		error_msg = "A table in the database is locked";
		break;
	case SQLITE_READONLY:
		error_msg = "Attempt to write a readonly database";
		break;
	case SQLITE_INTERRUPT:
		error_msg = "Operation terminated by sqlite3_interrupt(";
		break;
	case SQLITE_IOERR:
		error_msg = "Some kind of disk I/O error occurred";
		break;
	case SQLITE_CORRUPT:
		error_msg = "The database disk image is malformed";
		break;
	case SQLITE_NOTFOUND:
		error_msg = "Unknown opcode in sqlite3_file_control()";
		break;
	case SQLITE_FULL:
		error_msg = "Insertion failed because database is full";
		break;
	case SQLITE_CANTOPEN:
		error_msg = "Unable to open the database file";
		break;
	case SQLITE_PROTOCOL:
		error_msg = "Database lock protocol error";
		break;
	case SQLITE_EMPTY:
		error_msg = "Internal use only";
		break;
	case SQLITE_SCHEMA:
		error_msg = "The database schema changed";
		break;
	case SQLITE_CONSTRAINT:
		error_msg = "Abort due to constraint violation";
		break;
	case SQLITE_MISMATCH:
		error_msg = "Data type mismatch";
		break;
	case SQLITE_MISUSE:
		error_msg = "Library used incorrectly";
		break;
	case SQLITE_NOLFS:
		error_msg = "Uses OS features not supported on host";
		break;
	case SQLITE_AUTH:
		error_msg = "Authorization denied";
		break;
	case SQLITE_FORMAT:
		error_msg = "Not used";
		break;
	case SQLITE_RANGE:
		error_msg = "2nd parameter to sqlite3_bind out of range";
		break;
	case SQLITE_NOTADB:
		error_msg = "File opened that is not a database file";
		break;
	default:
		error_msg = "unknown error code";
		break;
	}
	sqlite3_result_error(context, error_msg.c_str(), error_msg.size());
}

SQLITE_API void sqlite3_result_int(sqlite3_context *context, int val) {
	sqlite3_result_int64(context, val);
}

SQLITE_API void sqlite3_result_int64(sqlite3_context *context, sqlite3_int64 val) {
	context->result.u.i = val;
	context->result.type = SQLiteTypeValue::INTEGER;
}

SQLITE_API void sqlite3_result_null(sqlite3_context *context) {
	context->result.type = SQLiteTypeValue::NULL_VALUE;
}

SQLITE_API void sqlite3_result_text(sqlite3_context *context, const char *str_c, int n_chars, void (*xDel)(void *)) {
	if (!str_c) {
		context->isError = SQLITE_MISUSE;
		return;
	}
	if (n_chars < 0) {
		n_chars = strlen(str_c);
	}

	auto utf_type = Utf8Proc::Analyze(str_c, n_chars);
	if (utf_type == UnicodeType::INVALID) {
		context->isError = SQLITE_MISUSE;
		return;
	}
	context->result = CastToSQLiteValue::Operation<string_t>(string_t(str_c, n_chars));
	if (xDel && xDel != SQLITE_TRANSIENT) {
		xDel((void *)str_c);
	}
}

SQLITE_API void sqlite3_result_text64(sqlite3_context *, const char *, sqlite3_uint64, void (*)(void *),
                                      unsigned char encoding) {
}

SQLITE_API void sqlite3_result_text16(sqlite3_context *, const void *, int, void (*)(void *)) {
}

SQLITE_API void sqlite3_result_text16le(sqlite3_context *, const void *, int, void (*)(void *)) {
}

SQLITE_API void sqlite3_result_text16be(sqlite3_context *, const void *, int, void (*)(void *)) {
}

SQLITE_API void sqlite3_result_value(sqlite3_context *, sqlite3_value *) {
}

SQLITE_API void sqlite3_result_pointer(sqlite3_context *, void *, const char *, void (*)(void *)) {
}

SQLITE_API void sqlite3_result_zeroblob(sqlite3_context *, int n) {
}

SQLITE_API int sqlite3_result_zeroblob64(sqlite3_context *, sqlite3_uint64 n) {
	return -1;
}

const void *sqlite3_value_blob(sqlite3_value *pVal) {
	return sqlite3_value_text(pVal);
}

double sqlite3_value_double(sqlite3_value *pVal) {
	if (!pVal) {
		pVal->db->errCode = SQLITE_MISUSE;
		return 0.0;
	}
	switch (pVal->type) {
	case SQLiteTypeValue::FLOAT:
		return pVal->u.r;
	case SQLiteTypeValue::INTEGER:
		return (double)pVal->u.i;
	case SQLiteTypeValue::TEXT:
	case SQLiteTypeValue::BLOB:
		double res;
		if (TryCast::Operation<string_t, double>(string_t(pVal->str), res)) {
			return res;
		}
		break;
	default:
		break;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0.0;
}

int sqlite3_value_int(sqlite3_value *pVal) {
	int64_t res = sqlite3_value_int64(pVal);
	if (res >= NumericLimits<int>::Minimum() && res <= NumericLimits<int>::Maximum()) {
		return res;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0;
}

sqlite3_int64 sqlite3_value_int64(sqlite3_value *pVal) {
	if (!pVal) {
		pVal->db->errCode = SQLITE_MISUSE;
		return 0;
	}
	int64_t res;
	switch (pVal->type) {
	case SQLiteTypeValue::INTEGER:
		return pVal->u.i;
	case SQLiteTypeValue::FLOAT:
		if (TryCast::Operation<double, int64_t>(pVal->u.r, res)) {
			return res;
		}
		break;
	case SQLiteTypeValue::TEXT:
	case SQLiteTypeValue::BLOB:
		if (TryCast::Operation<string_t, int64_t>(string_t(pVal->str), res)) {
			return res;
		}
		break;
	default:
		break;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0;
}

void *sqlite3_value_pointer(sqlite3_value *, const char *) {
	return nullptr;
}

const unsigned char *sqlite3_value_text(sqlite3_value *pVal) {
	if (!pVal) {
		pVal->db->errCode = SQLITE_MISUSE;
		return nullptr;
	}
	if (pVal->type == SQLiteTypeValue::TEXT || pVal->type == SQLiteTypeValue::BLOB) {
		return (const unsigned char *)pVal->str.c_str();
	}

	if (pVal->type == SQLiteTypeValue::INTEGER || pVal->type == SQLiteTypeValue::FLOAT) {
		Value value = (pVal->type == SQLiteTypeValue::INTEGER) ? Value::BIGINT(pVal->u.i) : Value::DOUBLE(pVal->u.r);
		if (!value.DefaultTryCastAs(LogicalType::VARCHAR)) {
			pVal->db->errCode = SQLITE_NOMEM;
			return nullptr;
		}
		auto &str_val = StringValue::Get(value);
		*pVal = CastToSQLiteValue::Operation<string_t>(string_t(str_val));
		return (const unsigned char *)pVal->str.c_str();
	}
	if (pVal->type == SQLiteTypeValue::NULL_VALUE) {
		return nullptr;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return nullptr;
}

SQLITE_API const void *sqlite3_value_text16(sqlite3_value *) {
	return nullptr;
}

SQLITE_API const void *sqlite3_value_text16le(sqlite3_value *) {
	return nullptr;
}

SQLITE_API const void *sqlite3_value_text16be(sqlite3_value *) {
	return nullptr;
}

SQLITE_API int sqlite3_value_bytes(sqlite3_value *pVal) {
	if (pVal->type == SQLiteTypeValue::TEXT || pVal->type == SQLiteTypeValue::BLOB) {
		return pVal->str.size();
	}
	return 0;
}

SQLITE_API int sqlite3_value_bytes16(sqlite3_value *) {
	return 0;
}

SQLITE_API int sqlite3_value_type(sqlite3_value *pVal) {
	return (int)pVal->type;
}

SQLITE_API int sqlite3_value_numeric_type(sqlite3_value *) {
	return 0;
}

SQLITE_API int sqlite3_value_nochange(sqlite3_value *) {
	return 0;
}

SQLITE_API sqlite3_value *sqlite3_value_dup(const sqlite3_value *val) {
	return new sqlite3_value(*val);
}

SQLITE_API void sqlite3_value_free(sqlite3_value *val) {
	if (!val) {
		return;
	}
	delete val;
}

SQLITE_API void *sqlite3_aggregate_context(sqlite3_context *, int nBytes) {
	fprintf(stderr, "sqlite3_aggregate_context: unsupported.\n");

	return nullptr;
}

SQLITE_API int sqlite3_create_collation(sqlite3 *, const char *zName, int eTextRep, void *pArg,
                                        int (*xCompare)(void *, int, const void *, int, const void *)) {
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_create_window_function(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep,
                                              void *pApp, void (*xStep)(sqlite3_context *, int, sqlite3_value **),
                                              void (*xFinal)(sqlite3_context *), void (*xValue)(sqlite3_context *),
                                              void (*xInverse)(sqlite3_context *, int, sqlite3_value **),
                                              void (*xDestroy)(void *)) {
	// commented for now because such error message prevents the shell-test.py to pass
	//	fprintf(stderr, "sqlite3_create_window_function: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API sqlite3 *sqlite3_db_handle(sqlite3_stmt *s) {
	return s->db;
}

SQLITE_API char *sqlite3_expanded_sql(sqlite3_stmt *pStmt) {
	fprintf(stderr, "sqlite3_expanded_sql: unsupported.\n");
	return nullptr;
}

SQLITE_API int sqlite3_keyword_check(const char *str, int len) {
	return KeywordHelper::IsKeyword(std::string(str, len));
}

SQLITE_API int sqlite3_keyword_count(void) {
	fprintf(stderr, "sqlite3_keyword_count: unsupported.\n");
	return 0;
}

SQLITE_API int sqlite3_keyword_name(int, const char **, int *) {
	fprintf(stderr, "sqlite3_keyword_name: unsupported.\n");
	return 0;
}

SQLITE_API void sqlite3_progress_handler(sqlite3 *, int, int (*)(void *), void *) {
	fprintf(stderr, "sqlite3_progress_handler: unsupported.\n");
}

SQLITE_API int sqlite3_stmt_isexplain(sqlite3_stmt *pStmt) {
	if (!pStmt || !pStmt->prepared) {
		return 0;
	}
	return pStmt->prepared->GetStatementType() == StatementType::EXPLAIN_STATEMENT;
}

SQLITE_API int sqlite3_vtab_config(sqlite3 *, int op, ...) {
	fprintf(stderr, "sqlite3_vtab_config: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_busy_handler(sqlite3 *, int (*)(void *, int), void *) {
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_get_table(sqlite3 *db,       /* An open database */
                                 const char *zSql,  /* SQL to be evaluated */
                                 char ***pazResult, /* Results of the query */
                                 int *pnRow,        /* Number of result rows written here */
                                 int *pnColumn,     /* Number of result columns written here */
                                 char **pzErrmsg    /* Error msg written here */
) {
	fprintf(stderr, "sqlite3_get_table: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API void sqlite3_free_table(char **result) {
	fprintf(stderr, "sqlite3_free_table: unsupported.\n");
}

SQLITE_API int sqlite3_prepare(sqlite3 *db,           /* Database handle */
                               const char *zSql,      /* SQL statement, UTF-8 encoded */
                               int nByte,             /* Maximum length of zSql in bytes. */
                               sqlite3_stmt **ppStmt, /* OUT: Statement handle */
                               const char **pzTail    /* OUT: Pointer to unused portion of zSql */
) {
	return sqlite3_prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

SQLITE_API void *sqlite3_trace(sqlite3 *, void (*xTrace)(void *, const char *), void *) {
	fprintf(stderr, "sqlite3_trace: unsupported.\n");
	return nullptr;
}

SQLITE_API void *sqlite3_profile(sqlite3 *, void (*xProfile)(void *, const char *, sqlite3_uint64), void *) {
	fprintf(stderr, "sqlite3_profile: unsupported.\n");
	return nullptr;
}

SQLITE_API int sqlite3_libversion_number(void) {
	return SQLITE_VERSION_NUMBER;
}

SQLITE_API int sqlite3_threadsafe(void) {
	return SQLITE_OK;
}

SQLITE_API sqlite3_mutex *sqlite3_mutex_alloc(int) {
	fprintf(stderr, "sqlite3_mutex_alloc: unsupported.\n");
	return nullptr;
}

SQLITE_API void sqlite3_mutex_free(sqlite3_mutex *) {
	fprintf(stderr, "sqlite3_mutex_free: unsupported.\n");
}

SQLITE_API int sqlite3_extended_result_codes(sqlite3 *db, int onoff) {
	fprintf(stderr, "sqlite3_extended_result_codes: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API void *sqlite3_update_hook(sqlite3 *db, /* Attach the hook to this database */
                                     void (*xCallback)(void *, int, char const *, char const *, sqlite_int64),
                                     void *pArg /* Argument to the function */
) {
	fprintf(stderr, "sqlite3_update_hook: unsupported.\n");
	return nullptr;
}

SQLITE_API void sqlite3_log(int iErrCode, const char *zFormat, ...) {
	fprintf(stderr, "sqlite3_log: unsupported.\n");
}

SQLITE_API int sqlite3_unlock_notify(sqlite3 *db, void (*xNotify)(void **, int), void *pArg) {
	fprintf(stderr, "sqlite3_unlock_notify: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API void *sqlite3_get_auxdata(sqlite3_context *pCtx, int iArg) {
	fprintf(stderr, "sqlite3_get_auxdata: unsupported.\n");
	return nullptr;
}

SQLITE_API void *sqlite3_rollback_hook(sqlite3 *db,               /* Attach the hook to this database */
                                       void (*xCallback)(void *), /* Callback function */
                                       void *pArg                 /* Argument to the function */
) {
	fprintf(stderr, "sqlite3_rollback_hook: unsupported.\n");
	return nullptr;
}

SQLITE_API void *sqlite3_commit_hook(sqlite3 *db,              /* Attach the hook to this database */
                                     int (*xCallback)(void *), /* Function to invoke on each commit */
                                     void *pArg                /* Argument to the function */
) {
	fprintf(stderr, "sqlite3_commit_hook: unsupported.\n");
	return nullptr;
}

SQLITE_API int sqlite3_blob_open(sqlite3 *db,          /* The database connection */
                                 const char *zDb,      /* The attached database containing the blob */
                                 const char *zTable,   /* The table containing the blob */
                                 const char *zColumn,  /* The column containing the blob */
                                 sqlite_int64 iRow,    /* The row containing the glob */
                                 int wrFlag,           /* True -> read/write access, false -> read-only */
                                 sqlite3_blob **ppBlob /* Handle for accessing the blob returned here */
) {
	fprintf(stderr, "sqlite3_blob_open: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API const char *sqlite3_db_filename(sqlite3 *db, const char *zDbName) {
	fprintf(stderr, "sqlite3_db_filename: unsupported.\n");
	return nullptr;
}

SQLITE_API int sqlite3_stmt_busy(sqlite3_stmt *) {
	fprintf(stderr, "sqlite3_stmt_busy: unsupported.\n");
	return false;
}

SQLITE_API int sqlite3_bind_pointer(sqlite3_stmt *pStmt, int i, void *pPtr, const char *zPTtype,
                                    void (*xDestructor)(void *)) {
	fprintf(stderr, "sqlite3_bind_pointer: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_create_module_v2(sqlite3 *db,                   /* Database in which module is registered */
                                        const char *zName,             /* Name assigned to this module */
                                        const sqlite3_module *pModule, /* The definition of the module */
                                        void *pAux,                    /* Context pointer for xCreate/xConnect */
                                        void (*xDestroy)(void *)       /* Module destructor function */
) {
	fprintf(stderr, "sqlite3_create_module_v2: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_blob_write(sqlite3_blob *, const void *z, int n, int iOffset) {
	fprintf(stderr, "sqlite3_blob_write: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API void sqlite3_set_auxdata(sqlite3_context *, int N, void *, void (*)(void *)) {
	fprintf(stderr, "sqlite3_set_auxdata: unsupported.\n");
}

SQLITE_API sqlite3_stmt *sqlite3_next_stmt(sqlite3 *pDb, sqlite3_stmt *pStmt) {
	fprintf(stderr, "sqlite3_next_stmt: unsupported.\n");
	return nullptr;
}

SQLITE_API int sqlite3_collation_needed(sqlite3 *, void *, void (*)(void *, sqlite3 *, int eTextRep, const char *)) {
	fprintf(stderr, "sqlite3_collation_needed: unsupported.\n");
	return SQLITE_ERROR;
}

SQLITE_API int sqlite3_create_collation_v2(sqlite3 *, const char *zName, int eTextRep, void *pArg,
                                           int (*xCompare)(void *, int, const void *, int, const void *),
                                           void (*xDestroy)(void *)) {
	fprintf(stderr, "sqlite3_create_collation_v2: unsupported.\n");
	return SQLITE_ERROR;
}
