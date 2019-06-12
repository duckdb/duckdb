#include "duckdb.hpp"
#include "main/appender.hpp"

#include <Rdefines.h>
// motherfucker
#undef error

using namespace duckdb;
using namespace std;

// converter for primitive types
template <class SRC, class DEST>
static void vector_to_r(Vector &src_vec, void *dest, uint64_t dest_offset, DEST na_val) {
	DEST *dest_ptr = ((DEST *)dest) + dest_offset;
	for (size_t row_idx = 0; row_idx < src_vec.count; row_idx++) {
		dest_ptr[row_idx] = src_vec.nullmask[row_idx] ? na_val : ((SRC *)src_vec.data)[row_idx];
	}
}

extern "C" {

SEXP duckdb_query_R(SEXP connsexp, SEXP querysexp) {
	if (TYPEOF(querysexp) != STRSXP || LENGTH(querysexp) != 1) {
		Rf_error("duckdb_query_R: Need single string parameter for query");
	}
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_query_R: Need external pointer parameter for connections");
	}

	char *query = (char *)CHAR(STRING_ELT(querysexp, 0));
	if (!query) {
		Rf_error("duckdb_query_R: No query");
	}

	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_query_R: Invalid connection");
	}

	// step 1: run query
	// need materialized result because we need a count for the R data frame :/
	auto result = conn->Query(query);

	if (!result->success) {
		Rf_error("duckdb_query_R: Error: %s", result->error.c_str());
	}

	// step 2: create result data frame and allocate columns
	uint32_t ncols = result->types.size();
	uint64_t nrows = result->collection.count;

	if (ncols > 0) {
		SEXP retlist = PROTECT(NEW_LIST(ncols));
		if (!retlist) {
			UNPROTECT(1); // retlist
			Rf_error("duckdb_query_R: Memory allocation failed");
		}
		SEXP names = PROTECT(NEW_STRING(ncols));
		if (!names) {
			UNPROTECT(2); // retlist, names
			Rf_error("duckdb_query_R: Memory allocation failed");
		}
		SET_NAMES(retlist, names);
		UNPROTECT(1); // names

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			SEXP varname = PROTECT(mkCharCE(result->names[col_idx].c_str(), CE_UTF8));
			if (!varname) {
				UNPROTECT(2); // varname, retlist
				Rf_error("duckdb_query_R: Memory allocation failed");
			}
			SET_STRING_ELT(names, col_idx, varname);
			UNPROTECT(1); // varname

			SEXP varvalue = NULL;
			switch (result->sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				varvalue = PROTECT(NEW_LOGICAL(nrows));
				break;
			case SQLTypeId::TINYINT:
			case SQLTypeId::SMALLINT:
			case SQLTypeId::INTEGER:
				varvalue = PROTECT(NEW_INTEGER(nrows));
				break;
			case SQLTypeId::BIGINT:
			case SQLTypeId::FLOAT:
			case SQLTypeId::DOUBLE:
			case SQLTypeId::DECIMAL:
				varvalue = PROTECT(NEW_NUMERIC(nrows));
				break;
			case SQLTypeId::VARCHAR:
				varvalue = PROTECT(NEW_STRING(nrows));
				break;
			default:
				UNPROTECT(1); // retlist
				Rf_error("duckdb_query_R: Unknown column type %s", TypeIdToString(result->types[col_idx]).c_str());
			}
			if (!varvalue) {
				UNPROTECT(2); // varvalue, retlist
				Rf_error("duckdb_query_R: Memory allocation failed");
			}
			SET_VECTOR_ELT(retlist, col_idx, varvalue);
			UNPROTECT(1); /* varvalue */
		}

		// at this point retlist is fully allocated and the only protected SEXP

		// step 3: set values from chunks
		uint64_t dest_offset = 0;
		while (true) {
			auto chunk = result->Fetch();
			if (chunk->size() == 0) {
				break;
			}
			assert(chunk->column_count == ncols);
			assert(chunk->column_count == LENGTH(retlist));
			for (size_t col_idx = 0; col_idx < chunk->column_count; col_idx++) {
				SEXP dest = VECTOR_ELT(retlist, col_idx);
				switch (chunk->GetTypes()[col_idx]) {
				case TypeId::BOOLEAN:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], LOGICAL_POINTER(dest), dest_offset,
					                               NA_LOGICAL);
					break;
				case TypeId::TINYINT:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset,
					                               NA_INTEGER);
					break;
				case TypeId::SMALLINT:
					vector_to_r<int16_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset,
					                                NA_INTEGER);
					break;
				case TypeId::INTEGER:
					vector_to_r<int32_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset,
					                                NA_INTEGER);
					break;
				case TypeId::BIGINT:
					vector_to_r<int64_t, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;
				case TypeId::FLOAT:
					vector_to_r<float, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;
				case TypeId::DOUBLE:
					vector_to_r<double, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;
				case TypeId::VARCHAR:
					for (size_t row_idx = 0; row_idx < chunk->data[col_idx].count; row_idx++) {
						char **src_ptr = ((char **)chunk->data[col_idx].data);
						if (chunk->data[col_idx].nullmask[row_idx]) {
							SET_STRING_ELT(dest, dest_offset + row_idx, NA_STRING);
						} else {
							SET_STRING_ELT(dest, dest_offset + row_idx, mkCharCE(src_ptr[row_idx], CE_UTF8));
						}
					}
					break;
				default:
					Rf_error("duckdb_query_R: Unknown column type %s",
					         TypeIdToString(chunk->GetTypes()[col_idx]).c_str());
					break;
				}
			}
			dest_offset += chunk->size();
		}

		assert(dest_offset == nrows);
		UNPROTECT(1); /* retlist */
		return retlist;
	}
	return ScalarReal(0); // no need for protection because no allocation can happen afterwards
}

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		warning("duckdb_finalize_database_R: Database is garbage-collected, use xxx to avoid this.");
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}
	return R_NilValue;
}

SEXP duckdb_startup_R(SEXP dbdirsexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || LENGTH(dbdirsexp) != 1) {
		Rf_error("duckdb_startup_R: Need single string parameter");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));
	if (strlen(dbdir) == 0 || strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	DuckDB *dbaddr;
	try {
		dbaddr = new DuckDB(dbdir);
	} catch (...) {
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
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}

	return R_NilValue;
}

static SEXP duckdb_finalize_connection_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	Connection *connaddr = (Connection *)R_ExternalPtrAddr(connsexp);
	if (connaddr) {
		warning("duckdb_finalize_connection_R: Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		delete connaddr;
	}
	return R_NilValue;
}

SEXP duckdb_connect_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_connect_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (!dbaddr) {
		Rf_error("duckdb_connect_R: Invalid database reference");
	}

	SEXP conn = PROTECT(R_MakeExternalPtr(new Connection(*dbaddr), R_NilValue, R_NilValue));
	R_RegisterCFinalizer(conn, (void (*)(SEXP))duckdb_finalize_connection_R);
	UNPROTECT(1);

	return conn;
}

SEXP duckdb_disconnect_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_disconnect_R: Need external pointer parameter");
	}
	Connection *connaddr = (Connection *)R_ExternalPtrAddr(connsexp);
	if (connaddr) {
		R_ClearExternalPtr(connsexp);
		delete connaddr;
	}
	return R_NilValue;
}

SEXP duckdb_append_R(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_append_R: Need external pointer parameter for connection");
	}

	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_append_R: Invalid connection");
	}

	if (TYPEOF(namesexp) != STRSXP || LENGTH(namesexp) != 1) {
		Rf_error("duckdb_append_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));

	// FIXME crude way of stripping quotes, what about escaped quotes?
	if (name.front() == '"') {
		name.erase(0, 1);
		name.erase(name.size() - 1);
	}

	if (TYPEOF(valuesexp) != VECSXP || LENGTH(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		Rf_error("duckdb_append_R: Need at least one-column data frame parameter for value");
	}

	// TODO transaction :/
	try {
		Appender appender(conn->db, DEFAULT_SCHEMA, name);
		auto nrows = LENGTH(VECTOR_ELT(valuesexp, 0));
		for (uint64_t row_idx = 0; row_idx < nrows; row_idx++) {
			appender.BeginRow();
			for (uint32_t col_idx = 0; col_idx < LENGTH(valuesexp); col_idx++) {
				SEXP coldata = VECTOR_ELT(valuesexp, col_idx);

				// TODO date time etc. types, ...
				if (isFactor(coldata) && TYPEOF(coldata) == INTSXP) {
					int val = INTEGER_POINTER(coldata)[row_idx];
					if (val == NA_INTEGER) {
						appender.AppendValue(Value());
					} else {
						SEXP factor_levels = GET_LEVELS(coldata);
						appender.AppendString(CHAR(STRING_ELT(factor_levels, val - 1)));
					}
				} else if (TYPEOF(coldata) == LGLSXP) {
					int val = INTEGER_POINTER(coldata)[row_idx];
					if (val == NA_INTEGER) {
						appender.AppendValue(Value()); // TODO add AppendNull to appender
					} else {
						appender.AppendTinyInt(val);
					}
				} else if (TYPEOF(coldata) == INTSXP) {
					int val = INTEGER_POINTER(coldata)[row_idx];
					if (val == NA_INTEGER) {
						appender.AppendValue(Value());
					} else {
						appender.AppendInteger(val);
					}
				} else if (TYPEOF(coldata) == REALSXP) {
					double val = NUMERIC_POINTER(coldata)[row_idx];
					if (val == NA_REAL) {
						appender.AppendValue(Value());
					} else {
						appender.AppendDouble(val);
					}
				} else if (TYPEOF(coldata) == STRSXP) {
					SEXP val = STRING_ELT(coldata, row_idx);
					if (val == NA_STRING) {
						appender.AppendValue(Value());
					} else {
						appender.AppendString(CHAR(val));
					}
				} else {
					throw;
				}
			}
			appender.EndRow();
		}

		appender.Commit();
	} catch (...) {
		Rf_error("duckdb_append_R: Failed to append data");
	}

	return R_NilValue;
}

SEXP duckdb_ptr_to_str(SEXP extptr) {
	if (TYPEOF(extptr) != EXTPTRSXP) {
		Rf_error("duckdb_ptr_to_str: Need external pointer parameter");
	}
	SEXP ret = PROTECT(NEW_STRING(1));
	SET_STRING_ELT(ret, 0, NA_STRING);
	void *ptr = R_ExternalPtrAddr(extptr);
	if (ptr != NULL) {
		char buf[100];
		snprintf(buf, 100, "%p", ptr);
		SET_STRING_ELT(ret, 0, mkChar(buf));
	}
	UNPROTECT(1);
	return ret;
}

// R native routine registration
#define CALLDEF(name, n)                                                                                               \
	{ #name, (DL_FUNC)&name, n }
static const R_CallMethodDef R_CallDef[] = {CALLDEF(duckdb_startup_R, 1),
                                            CALLDEF(duckdb_connect_R, 1),
                                            CALLDEF(duckdb_query_R, 2),
                                            CALLDEF(duckdb_append_R, 3),
                                            CALLDEF(duckdb_disconnect_R, 1),
                                            CALLDEF(duckdb_shutdown_R, 1),
                                            CALLDEF(duckdb_ptr_to_str, 1),

                                            {NULL, NULL, 0}};

void R_init_duckdb(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);
}
}
