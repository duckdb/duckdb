#include "duckdb.hpp"

#include <Rdefines.h>
#include <algorithm>

// motherfucker
#undef error

using namespace duckdb;
using namespace std;

// converter for primitive types
template <class SRC, class DEST>
static void vector_to_r(Vector &src_vec, void *dest, uint64_t dest_offset, DEST na_val) {
	auto src_ptr = (SRC *)src_vec.GetData();
	auto dest_ptr = ((DEST *)dest) + dest_offset;
	for (size_t row_idx = 0; row_idx < src_vec.size(); row_idx++) {
		dest_ptr[row_idx] = src_vec.nullmask[row_idx] ? na_val : src_ptr[row_idx];
	}
}

struct RDoubleType {
	static bool IsNull(double val) {
		return ISNA(val);
	}

	static double Convert(double val) {
		return val;
	}
};

struct RDateType {
	static bool IsNull(double val) {
		return RDoubleType::IsNull(val);
	}

	static double Convert(double val) {
		return (date_t)val + 719528; // MAGIC!
	}
};

struct RTimestampType {
	static bool IsNull(double val) {
		return RDoubleType::IsNull(val);
	}

	static timestamp_t Convert(double val) {
		date_t date = Date::EpochToDate((int64_t)val);
		dtime_t time = (dtime_t)(((int64_t)val % (60 * 60 * 24)) * 1000);
		return Timestamp::FromDatetime(date, time);
	}
};

struct RIntegerType {
	static bool IsNull(int val) {
		return val == NA_INTEGER;
	}

	static int Convert(int val) {
		return val;
	}
};

struct RBooleanType {
	static bool IsNull(int val) {
		return RIntegerType::IsNull(val);
	}

	static bool Convert(int val) {
		return val;
	}
};

template <class SRC, class DST, class RTYPE>
static void AppendColumnSegment(SRC *source_data, Vector &result, index_t count) {
	auto result_data = (DST *)result.GetData();
	for (index_t i = 0; i < count; i++) {
		auto val = source_data[i];
		if (RTYPE::IsNull(val)) {
			result.nullmask[i] = true;
		} else {
			result_data[i] = RTYPE::Convert(val);
		}
	}
}

static void AppendStringSegment(SEXP coldata, Vector &result, index_t row_idx, index_t count) {
	auto result_data = (string_t *)result.GetData();
	for (index_t i = 0; i < count; i++) {
		SEXP val = STRING_ELT(coldata, row_idx + i);
		if (val == NA_STRING) {
			result.nullmask[i] = true;
		} else {
			result_data[i] = string_t((char *)CHAR(val));
		}
	}
}

static void AppendFactor(SEXP coldata, Vector &result, index_t row_idx, index_t count) {
	auto source_data = INTEGER_POINTER(coldata) + row_idx;
	auto result_data = (string_t *)result.GetData();
	SEXP factor_levels = GET_LEVELS(coldata);
	for (index_t i = 0; i < count; i++) {
		int val = source_data[i];
		if (RIntegerType::IsNull(val)) {
			result.nullmask[i] = true;
		} else {
			result_data[i] = string_t(CHAR(STRING_ELT(factor_levels, val - 1)));
		}
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
		Rf_error("duckdb_query_R: Failed to run query %s\nError: %s", query, result->error.c_str());
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
			case SQLTypeId::TIMESTAMP:
			case SQLTypeId::DATE:
			case SQLTypeId::TIME:
				varvalue = PROTECT(NEW_NUMERIC(nrows));
				break;
			case SQLTypeId::VARCHAR:
				varvalue = PROTECT(NEW_STRING(nrows));
				break;
			default:
				UNPROTECT(1); // retlist
				Rf_error("duckdb_query_R: Unknown column type %s/%s",
				         SQLTypeToString(result->sql_types[col_idx]).c_str(),
				         TypeIdToString(result->types[col_idx]).c_str());
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
			assert(chunk->column_count() == ncols);
			assert(chunk->column_count() == LENGTH(retlist));
			for (size_t col_idx = 0; col_idx < chunk->column_count(); col_idx++) {
				SEXP dest = VECTOR_ELT(retlist, col_idx);
				switch (result->sql_types[col_idx].id) {
				case SQLTypeId::BOOLEAN:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], LOGICAL_POINTER(dest), dest_offset, NA_LOGICAL);
					break;
				case SQLTypeId::TINYINT:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset, NA_INTEGER);
					break;
				case SQLTypeId::SMALLINT:
					vector_to_r<int16_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset,
					                               NA_INTEGER);
					break;
				case SQLTypeId::INTEGER:
					vector_to_r<int32_t, uint32_t>(chunk->data[col_idx], INTEGER_POINTER(dest), dest_offset,
					                               NA_INTEGER);
					break;
				case SQLTypeId::TIMESTAMP: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = (int64_t *)src_vec.GetData();
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < src_vec.size(); row_idx++) {
						dest_ptr[row_idx] =
						    src_vec.nullmask[row_idx] ? NA_REAL : (double)Timestamp::GetEpoch(src_data[row_idx]);
					}

					// some dresssup for R
					SEXP cl = PROTECT(NEW_STRING(2));
					SET_STRING_ELT(cl, 0, PROTECT(mkChar("POSIXct")));
					SET_STRING_ELT(cl, 1, PROTECT(mkChar("POSIXt")));
					SET_CLASS(dest, cl);
					setAttrib(dest, install("tzone"), PROTECT(mkString("UTC")));
					UNPROTECT(4);
					break;
				}
				case SQLTypeId::DATE: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = (int32_t *)src_vec.GetData();
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < src_vec.size(); row_idx++) {
						dest_ptr[row_idx] = src_vec.nullmask[row_idx] ? NA_REAL : (double)(src_data[row_idx]) - 719528;
					}

					// some dresssup for R
					SET_CLASS(dest, PROTECT(mkString("Date")));
					UNPROTECT(1);
					break;
				}
				case SQLTypeId::TIME: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = (int32_t *)src_vec.GetData();
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < src_vec.size(); row_idx++) {

						if (src_vec.nullmask[row_idx]) {
							dest_ptr[row_idx] = NA_REAL;
						} else {
							time_t n = src_data[row_idx];
							int h;
							double frac;
							h = n / 3600000;
							n -= h * 3600000;
							frac = (n / 60000.0) / 60.0;
							dest_ptr[row_idx] = h + frac;
						}
					}

					// some dresssup for R
					SET_CLASS(dest, PROTECT(mkString("difftime")));
					setAttrib(dest, install("units"), PROTECT(mkString("hours")));
					UNPROTECT(2);
					break;
				}
				case SQLTypeId::BIGINT:
					vector_to_r<int64_t, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;
				case SQLTypeId::FLOAT:
					vector_to_r<float, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;

				case SQLTypeId::DOUBLE:
					vector_to_r<double, double>(chunk->data[col_idx], NUMERIC_POINTER(dest), dest_offset, NA_REAL);
					break;
				case SQLTypeId::VARCHAR: {
					for (size_t row_idx = 0; row_idx < chunk->data[col_idx].size(); row_idx++) {
						auto src_ptr = (string_t *)chunk->data[col_idx].GetData();
						if (chunk->data[col_idx].nullmask[row_idx]) {
							SET_STRING_ELT(dest, dest_offset + row_idx, NA_STRING);
						} else {
							SET_STRING_ELT(dest, dest_offset + row_idx, mkCharCE(src_ptr[row_idx].GetData(), CE_UTF8));
						}
					}
					break;
				}
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
		warning("duckdb_finalize_database_R: Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
		        "duckdb::duckdb_shutdown(drv) to avoid this.");
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}
	return R_NilValue;
}

SEXP duckdb_startup_R(SEXP dbdirsexp, SEXP readonlysexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || LENGTH(dbdirsexp) != 1) {
		Rf_error("duckdb_startup_R: Need string parameter for dbdir");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));

	if (TYPEOF(readonlysexp) != LGLSXP || LENGTH(readonlysexp) != 1) {
		Rf_error("duckdb_startup_R: Need string parameter for read_only");
	}
	bool read_only = (bool)LOGICAL_ELT(readonlysexp, 0);

	if (strlen(dbdir) == 0 || strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	DBConfig config;
	config.access_mode = AccessMode::READ_WRITE;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}
	DuckDB *dbaddr;
	try {
		dbaddr = new DuckDB(dbdir, &config);
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

	if (TYPEOF(valuesexp) != VECSXP || LENGTH(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		Rf_error("duckdb_append_R: Need at least one-column data frame parameter for value");
	}

	try {
		auto name = string(CHAR(STRING_ELT(namesexp, 0)));
		string schema, table;
		Catalog::ParseRangeVar(name, schema, table);
		if (schema == DEFAULT_SCHEMA) {
			schema = INVALID_SCHEMA;
		}

		Appender appender(*conn, schema, table);
		auto nrows = LENGTH(VECTOR_ELT(valuesexp, 0));
		for (index_t row_idx = 0; row_idx < nrows; row_idx += STANDARD_VECTOR_SIZE) {
			index_t current_count = std::min((index_t)nrows - row_idx, (index_t)STANDARD_VECTOR_SIZE);
			auto &append_chunk = appender.GetAppendChunk();
			for (index_t col_idx = 0; col_idx < LENGTH(valuesexp); col_idx++) {
				auto &append_data = append_chunk.data[col_idx];
				SEXP coldata = VECTOR_ELT(valuesexp, col_idx);
				if (TYPEOF(coldata) == REALSXP && TYPEOF(GET_CLASS(coldata)) == STRSXP &&
				    strcmp("POSIXct", CHAR(STRING_ELT(GET_CLASS(coldata), 0))) == 0) {
					// Timestamp
					auto data_ptr = NUMERIC_POINTER(coldata) + row_idx;
					AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, append_data, current_count);
				} else if (TYPEOF(coldata) == REALSXP && TYPEOF(GET_CLASS(coldata)) == STRSXP &&
				           strcmp("Date", CHAR(STRING_ELT(GET_CLASS(coldata), 0))) == 0) {
					// Date
					auto data_ptr = NUMERIC_POINTER(coldata) + row_idx;
					AppendColumnSegment<double, date_t, RDateType>(data_ptr, append_data, current_count);
				} else if (isFactor(coldata) && TYPEOF(coldata) == INTSXP) {
					// Factor
					AppendFactor(coldata, append_data, row_idx, current_count);
				} else if (TYPEOF(coldata) == LGLSXP) {
					// Boolean
					auto data_ptr = INTEGER_POINTER(coldata) + row_idx;
					AppendColumnSegment<int, bool, RBooleanType>(data_ptr, append_data, current_count);
				} else if (TYPEOF(coldata) == INTSXP) {
					// Integer
					auto data_ptr = INTEGER_POINTER(coldata) + row_idx;
					AppendColumnSegment<int, int, RIntegerType>(data_ptr, append_data, current_count);
				} else if (TYPEOF(coldata) == REALSXP) {
					// Double
					auto data_ptr = NUMERIC_POINTER(coldata) + row_idx;
					AppendColumnSegment<double, double, RDoubleType>(data_ptr, append_data, current_count);
				} else if (TYPEOF(coldata) == STRSXP) {
					// String
					AppendStringSegment(coldata, append_data, row_idx, current_count);
				} else {
					throw;
				}
			}
			append_chunk.SetCardinality(current_count);
			appender.Flush();
		}
		appender.Close();
	} catch (std::exception &ex) {
		Rf_error("duckdb_append_R failed: %s", ex.what());
	} catch (...) {
		Rf_error("duckdb_append_R failed: unknown error");
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
static const R_CallMethodDef R_CallDef[] = {CALLDEF(duckdb_startup_R, 2),
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
