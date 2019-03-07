#include "R_ext/Rallocators.h"
#include "duckdb.h"

#include <R_ext/Rdynload.h>
#include <Rdefines.h>
#include <Rinternals.h>

#define RSTR(somestr) mkCharCE(somestr, CE_UTF8)

SEXP duckdb_query_R(SEXP connsexp, SEXP querysexp) {
	duckdb_result output;
	//	long affected_rows = 0, prepare_id = 0;
	duckdb_state res;

	void *connptr = R_ExternalPtrAddr(connsexp);

	res = duckdb_query(connptr, (char *)CHAR(STRING_ELT(querysexp, 0)), &output);
	if (res != DuckDBSuccess) { // there was an error
		Rf_error("Query error");
	}

	if (output.column_count > 0) {
		int i = 0, ncols = output.column_count;
		SEXP retlist = NULL, names = NULL;
		PROTECT(retlist = NEW_LIST(ncols));
		if (!retlist) {
			UNPROTECT(1);
			Rf_error("Memory allocation failed");
		}
		PROTECT(names = NEW_STRING(ncols));
		if (!names) {
			UNPROTECT(2);
			Rf_error("Memory allocation failed");
		}

		for (size_t col_idx = 0; col_idx < output.column_count; col_idx++) {
			duckdb_column col = output.columns[col_idx];
			SEXP varvalue = NULL;
			SEXP varname = PROTECT(RSTR(col.name));
			if (!varname) {
// TODO				UNPROTECT(col_idx * 2 + 3);
				Rf_error("Memory allocation failed");
			}
			switch (col.type) {

			default:
				Rf_error("Unknown column type");
				break;
			}

//			if (duckdb_value_is_null(col, self->offset)) {
//				PyList_SetItem(row, col_idx, Py_None);
//				continue;
//			}
//			switch (col.type) {
//			case DUCKDB_TYPE_BOOLEAN:
//			case DUCKDB_TYPE_TINYINT:
//				val = Py_BuildValue("b", ((int8_t *)col.data)[self->offset]);
//				break;
//			case DUCKDB_TYPE_SMALLINT:
//				val = Py_BuildValue("h", ((int16_t *)col.data)[self->offset]);
//				break;
//			case DUCKDB_TYPE_INTEGER:
//				val = Py_BuildValue("i", ((int32_t *)col.data)[self->offset]);
//				break;
//			case DUCKDB_TYPE_BIGINT:
//				val = Py_BuildValue("L", ((int64_t *)col.data)[self->offset]);
//				break;
//			case DUCKDB_TYPE_DECIMAL:
//				val = Py_BuildValue("d", ((double *)col.data)[self->offset]);
//				break;
//			case DUCKDB_TYPE_VARCHAR:
//				val = Py_BuildValue("s", duckdb_get_value_str(col, self->offset));
//				break;
//			default:
//				// TODO complain
//				break;
//			}
//			if (val) {
//				Py_INCREF(val);
//				PyList_SetItem(row, col_idx, val);
//				Py_DECREF(val);
//			}

			if (!varvalue) {
				// TODO				UNPROTECT(col_idx * 2 + 3);
				Rf_error("Conversion failed");
			}

			SET_VECTOR_ELT(retlist, i, varvalue);
			SET_STRING_ELT(names, i, varname);
			UNPROTECT(2); /* varname, varvalue */
		}



		for (i = 0; i < ncols; i++) {



			//			if (!(varvalue = bat_to_sexp(b, output->nrows, &(raw_col->type), &unfix, LOGICAL(int64sexp)[0])))
			//{ 				UNPROTECT(i * 2 + 4); 				PutRNGstate(); 				return monetdb_error_R("Conversion error");
			//			}
			//			SET_VECTOR_ELT(retlist, i, varvalue);
			//			SET_STRING_ELT(names, i, varname);
			//
			//
			//			UNPROTECT(2); /* varname, varvalue */
		}
		//		SET_ATTR(retlist, install("__rows"), PROTECT(Rf_ScalarReal(nrows)));
		UNPROTECT(1);
		//		if (prepare_id > 0) {
		//			SET_ATTR(retlist, install("__prepare"), PROTECT(Rf_ScalarReal(prepare_id)));
		//			UNPROTECT(1);
		//		}

		duckdb_destroy_result(output);
		SET_NAMES(retlist, names);
		UNPROTECT(2); /* names, retlist */
		return retlist;
	}
	duckdb_destroy_result(output);
	return ScalarReal(0);
}

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	void *addr = R_ExternalPtrAddr(dbsexp);
	if (addr) {
		warning("Database is garbage-collected, use xxx to avoid this.");
		R_ClearExternalPtr(dbsexp);
		duckdb_close(addr);
	}
	return R_NilValue;
}

SEXP duckdb_startup_R(SEXP dbdirsexp) {
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));
	if (strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	duckdb_database db;
	duckdb_state res = duckdb_open(dbdir, &db);

	if (res != DuckDBSuccess) {
		Rf_error("Failed to open database");
	}

	SEXP dbsexp = PROTECT(R_MakeExternalPtr(db, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(dbsexp, (void (*)(SEXP))duckdb_finalize_database_R);
	UNPROTECT(1);
	return dbsexp;
}

SEXP duckdb_shutdown_R(SEXP dbsexp) {
	void *addr = R_ExternalPtrAddr(dbsexp);
	if (addr) {
		R_ClearExternalPtr(dbsexp);
		duckdb_close(addr);
	}

	return R_NilValue;
}

static SEXP duckdb_finalize_connection_R(SEXP connsexp) {
	void *addr = R_ExternalPtrAddr(connsexp);
	if (addr) {
		warning("Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		duckdb_disconnect(addr);
	}
	return R_NilValue;
}

SEXP duckdb_connect_R(SEXP dbsexp) {
	void *dbaddr = R_ExternalPtrAddr(dbsexp);
	if (!dbaddr) {
		Rf_error("Invalid database reference");
	}

	duckdb_connection connaddr;
	duckdb_state res = duckdb_connect((duckdb_database)dbaddr, &connaddr);
	if (res != DuckDBSuccess) {
		Rf_error("Could not create connection.");
	}

	SEXP conn = PROTECT(R_MakeExternalPtr(connaddr, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(conn, (void (*)(SEXP))duckdb_finalize_connection_R);
	UNPROTECT(1);

	return conn;
}

SEXP duckdb_disconnect_R(SEXP connsexp) {
	void *addr = R_ExternalPtrAddr(connsexp);
	if (addr) {
		R_ClearExternalPtr(connsexp);
		duckdb_disconnect(addr);
	} else {
		warning("Connection was already disconnected.");
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
