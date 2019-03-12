#include "R_ext/Rallocators.h"
#include "duckdb.h"

#include <R_ext/Rdynload.h>
#include <Rdefines.h>
#include <Rinternals.h>

#define RSTR(somestr) mkCharCE(somestr, CE_UTF8)

SEXP duckdb_query_R(SEXP connsexp, SEXP querysexp) {
	if (TYPEOF(querysexp) != STRSXP || LENGTH(querysexp) != 1) {
		Rf_error("duckdb_query_R: Need single string parameter for query");
	}
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_query_R: Need external pointer parameter for connections");
	}

	duckdb_result output;
	//	long affected_rows = 0, prepare_id = 0;
	duckdb_state res;

	void *connaddr = R_ExternalPtrAddr(connsexp);
	if (!connaddr) {
		Rf_error("duckdb_query_R: invalid connection");
	}

	res = duckdb_query(connaddr, (char *)CHAR(STRING_ELT(querysexp, 0)), &output);
	if (res != DuckDBSuccess) { // there was an error
		Rf_error("duckdb_query_R: Error: %s", output.error_message);
	}

	uint32_t ncols = output.column_count;

	if (ncols > 0) {
		SEXP retlist = NULL, names = NULL;
		PROTECT(retlist = NEW_LIST(ncols));
		if (!retlist) {
			UNPROTECT(1);
			Rf_error("duckdb_query_R: Memory allocation failed");
		}
		PROTECT(names = NEW_STRING(ncols));
		if (!names) {
			UNPROTECT(2);
			Rf_error("duckdb_query_R: Memory allocation failed");
		}

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			duckdb_column col = output.columns[col_idx];
			SEXP varvalue = NULL;
			SEXP varname = PROTECT(RSTR(col.name));
			if (!varname) {
				// TODO				UNPROTECT(col_idx * 2 + 3);
				Rf_error("duckdb_query_R: Memory allocation failed");
			}
			switch (col.type) {
			// TODO macro?
			case DUCKDB_TYPE_BOOLEAN:
				varvalue = PROTECT(NEW_LOGICAL(output.row_count));
					if (varvalue) {
						for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
							if (col.nullmask[row_idx]) {
								LOGICAL_POINTER(varvalue)[row_idx] = NA_LOGICAL;
							} else {
								LOGICAL_POINTER(varvalue)[row_idx] = (uint32_t)((uint8_t *)col.data)[row_idx];
							}
						}
					}
					break;
			case DUCKDB_TYPE_TINYINT:
				varvalue = PROTECT(NEW_INTEGER(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							INTEGER_POINTER(varvalue)[row_idx] = NA_INTEGER;
						} else {
							INTEGER_POINTER(varvalue)[row_idx] = (uint32_t)((uint8_t *)col.data)[row_idx];
						}
					}
				}
				break;
			case DUCKDB_TYPE_SMALLINT:
				varvalue = PROTECT(NEW_INTEGER(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							INTEGER_POINTER(varvalue)[row_idx] = NA_INTEGER;
						} else {
							INTEGER_POINTER(varvalue)[row_idx] = (uint32_t)((uint16_t *)col.data)[row_idx];
						}
					}
				}
				break;
			case DUCKDB_TYPE_INTEGER:
				varvalue = PROTECT(NEW_INTEGER(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							INTEGER_POINTER(varvalue)[row_idx] = NA_INTEGER;
						} else {
							INTEGER_POINTER(varvalue)[row_idx] = (uint32_t)((uint32_t *)col.data)[row_idx];
						}
					}
				}
				break;
			case DUCKDB_TYPE_BIGINT:
				varvalue = PROTECT(NEW_NUMERIC(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							NUMERIC_POINTER(varvalue)[row_idx] = NA_REAL;
						} else {
							NUMERIC_POINTER(varvalue)[row_idx] = (double)((uint64_t *)col.data)[row_idx];
						}
					}
				}
				break;
			case DUCKDB_TYPE_DECIMAL:
				varvalue = PROTECT(NEW_NUMERIC(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							NUMERIC_POINTER(varvalue)[row_idx] = NA_REAL;
						} else {
							NUMERIC_POINTER(varvalue)[row_idx] = ((double *)col.data)[row_idx];
						}
					}
				}
				break;

			case DUCKDB_TYPE_VARCHAR:
				varvalue = PROTECT(NEW_STRING(output.row_count));
				if (varvalue) {
					for (size_t row_idx = 0; row_idx < output.row_count; row_idx++) {
						if (col.nullmask[row_idx]) {
							SET_STRING_ELT(varvalue, row_idx, NA_STRING);
						} else {
							SET_STRING_ELT(varvalue, row_idx, PROTECT(RSTR(((char **)col.data)[row_idx])));
							UNPROTECT(1);
						}
					}
				}
				break;
			default:
				Rf_error("duckdb_query_R: Unknown column type");
				break;
			}

			if (!varvalue) {
				// TODO				UNPROTECT(col_idx * 2 + 3);
				Rf_error("duckdb_query_R: Conversion failed");
			}

			SET_VECTOR_ELT(retlist, col_idx, varvalue);
			SET_STRING_ELT(names, col_idx, varname);
			UNPROTECT(2); /* varname, varvalue */
		}

		duckdb_destroy_result(&output);
		SET_NAMES(retlist, names);
		UNPROTECT(2); /* names, retlist */
		return retlist;
	}
	duckdb_destroy_result(&output);
	return ScalarReal(0);
}

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	void *dbaddr = R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		warning("duckdb_finalize_database_R: Database is garbage-collected, use xxx to avoid this.");
		R_ClearExternalPtr(dbsexp);
		duckdb_close(dbaddr);
	}
	return R_NilValue;
}

SEXP duckdb_startup_R(SEXP dbdirsexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || LENGTH(dbdirsexp) != 1) {
		Rf_error("duckdb_startup_R: Need single string parameter");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));
	if (strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	duckdb_database dbaddr;
	duckdb_state res = duckdb_open(dbdir, &dbaddr);

	if (res != DuckDBSuccess) {
		Rf_error("duckdb_startup_R: Failed to open database");
	}

	SEXP dbsexp = PROTECT(R_MakeExternalPtr(dbaddr, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(dbsexp, (void (*)(SEXP))duckdb_finalize_database_R);
	UNPROTECT(1);
	return dbsexp;
}

SEXP duckdb_shutdown_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	void *dbaddr = R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		R_ClearExternalPtr(dbsexp);
		duckdb_close(dbaddr);
	}

	return R_NilValue;
}

static SEXP duckdb_finalize_connection_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	void *addr = R_ExternalPtrAddr(connsexp);
	if (addr) {
		warning("duckdb_finalize_connection_R: Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		duckdb_disconnect(addr);
	}
	return R_NilValue;
}

SEXP duckdb_connect_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_connect_R: Need external pointer parameter");
	}
	void *dbaddr = R_ExternalPtrAddr(dbsexp);
	if (!dbaddr) {
		Rf_error("duckdb_connect_R: Invalid database reference");
	}

	duckdb_connection connaddr;
	duckdb_state res = duckdb_connect((duckdb_database)dbaddr, &connaddr);
	if (res != DuckDBSuccess) {
		Rf_error("duckdb_connect_R: Could not create connection.");
	}

	SEXP conn = PROTECT(R_MakeExternalPtr(connaddr, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(conn, (void (*)(SEXP))duckdb_finalize_connection_R);
	UNPROTECT(1);

	return conn;
}

SEXP duckdb_disconnect_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_disconnect_R: Need external pointer parameter");
	}
	void *connaddr = R_ExternalPtrAddr(connsexp);
	if (connaddr) {
		R_ClearExternalPtr(connsexp);
		duckdb_disconnect(&connaddr);
	}
	return R_NilValue;
}

// R native routine registration
#define CALLDEF(name, n)                                                                                               \
	{ #name, (DL_FUNC)&name, n }
static const R_CallMethodDef R_CallDef[] = {CALLDEF(duckdb_startup_R, 1),  CALLDEF(duckdb_connect_R, 1),
                                            CALLDEF(duckdb_query_R, 2),    CALLDEF(duckdb_disconnect_R, 1),
                                            CALLDEF(duckdb_shutdown_R, 1), {NULL, NULL, 0}};

void R_init_duckdb(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);
}
