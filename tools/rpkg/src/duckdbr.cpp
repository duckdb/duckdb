#include "duckdb.h"
#include <sstream>
#include "parquet-extension.h"

#include <Rdefines.h>
#include <algorithm>

// motherfucker
#undef error

using namespace duckdb;
using namespace std;

struct RStatement {
	unique_ptr<PreparedStatement> stmt;
	vector<Value> parameters;
};

// converter for primitive types
template <class SRC, class DEST>
static void vector_to_r(Vector &src_vec, size_t count, void *dest, uint64_t dest_offset, DEST na_val) {
	auto src_ptr = FlatVector::GetData<SRC>(src_vec);
	auto &nullmask = FlatVector::Nullmask(src_vec);
	auto dest_ptr = ((DEST *)dest) + dest_offset;
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] = nullmask[row_idx] ? na_val : src_ptr[row_idx];
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
static void AppendColumnSegment(SRC *source_data, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<DST>(result);
	auto &result_mask = FlatVector::Nullmask(result);
	for (idx_t i = 0; i < count; i++) {
		auto val = source_data[i];
		if (RTYPE::IsNull(val)) {
			result_mask[i] = true;
		} else {
			result_data[i] = RTYPE::Convert(val);
		}
	}
}

static void AppendStringSegment(SEXP coldata, Vector &result, idx_t row_idx, idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Nullmask(result);
	for (idx_t i = 0; i < count; i++) {
		SEXP val = STRING_ELT(coldata, row_idx + i);
		if (val == NA_STRING) {
			result_mask[i] = true;
		} else {
			result_data[i] = string_t((char *)CHAR(val));
		}
	}
}

static void AppendFactor(SEXP coldata, Vector &result, idx_t row_idx, idx_t count) {
	auto source_data = INTEGER_POINTER(coldata) + row_idx;
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Nullmask(result);
	SEXP factor_levels = GET_LEVELS(coldata);
	for (idx_t i = 0; i < count; i++) {
		int val = source_data[i];
		if (RIntegerType::IsNull(val)) {
			result_mask[i] = true;
		} else {
			result_data[i] = string_t(CHAR(STRING_ELT(factor_levels, val - 1)));
		}
	}
}

static SEXP cstr_to_charsexp(const char *s) {
	return mkCharCE(s, CE_UTF8);
}

static SEXP cpp_str_to_charsexp(string s) {
	return cstr_to_charsexp(s.c_str());
}

static SEXP cpp_str_to_strsexp(vector<string> s) {
	SEXP retsexp = PROTECT(NEW_STRING(s.size()));
	for (idx_t i = 0; i < s.size(); i++) {
		SET_STRING_ELT(retsexp, i, cpp_str_to_charsexp(s[i]));
	}
	UNPROTECT(1);
	return retsexp;
}

enum class RType { UNKNOWN, LOGICAL, INTEGER, NUMERIC, STRING, FACTOR, DATE, TIMESTAMP };

static RType detect_rtype(SEXP v) {
	if (TYPEOF(v) == REALSXP && TYPEOF(GET_CLASS(v)) == STRSXP &&
	    strcmp("POSIXct", CHAR(STRING_ELT(GET_CLASS(v), 0))) == 0) {
		return RType::TIMESTAMP;
	} else if (TYPEOF(v) == REALSXP && TYPEOF(GET_CLASS(v)) == STRSXP &&
	           strcmp("Date", CHAR(STRING_ELT(GET_CLASS(v), 0))) == 0) {
		return RType::DATE;
	} else if (isFactor(v) && TYPEOF(v) == INTSXP) {
		return RType::FACTOR;
	} else if (TYPEOF(v) == LGLSXP) {
		return RType::LOGICAL;
	} else if (TYPEOF(v) == INTSXP) {
		return RType::INTEGER;
	} else if (TYPEOF(v) == REALSXP) {
		return RType::NUMERIC;
	} else if (TYPEOF(v) == STRSXP) {
		return RType::STRING;
	}
	return RType::UNKNOWN;
}

extern "C" {

SEXP duckdb_release_R(SEXP stmtsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_release_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (stmtsexp) {
		R_ClearExternalPtr(stmtsexp);
		delete stmtholder;
	}
	return R_NilValue;
}

SEXP duckdb_finalize_statement_R(SEXP stmtsexp) {
	return duckdb_release_R(stmtsexp);
}

SEXP duckdb_prepare_R(SEXP connsexp, SEXP querysexp) {
	if (TYPEOF(querysexp) != STRSXP || LENGTH(querysexp) != 1) {
		Rf_error("duckdb_prepare_R: Need single string parameter for query");
	}
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_prepare_R: Need external pointer parameter for connections");
	}

	char *query = (char *)CHAR(STRING_ELT(querysexp, 0));
	if (!query) {
		Rf_error("duckdb_prepare_R: No query");
	}

	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_prepare_R: Invalid connection");
	}

	auto stmt = conn->Prepare(query);
	if (!stmt->success) {
		Rf_error("duckdb_prepare_R: Failed to prepare query %s\nError: %s", query, stmt->error.c_str());
	}

	auto stmtholder = new RStatement();
	stmtholder->stmt = move(stmt);

	SEXP retlist = PROTECT(NEW_LIST(6));

	SEXP stmtsexp = PROTECT(R_MakeExternalPtr(stmtholder, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(stmtsexp, (void (*)(SEXP))duckdb_finalize_statement_R);

	SEXP ret_names = cpp_str_to_strsexp({"str", "ref", "type", "names", "rtypes", "n_param"});
	SET_NAMES(retlist, ret_names);

	SET_VECTOR_ELT(retlist, 0, querysexp);
	SET_VECTOR_ELT(retlist, 1, stmtsexp);
	UNPROTECT(1); // stmtsxp

	SEXP stmt_type = cpp_str_to_strsexp({StatementTypeToString(stmtholder->stmt->type)});
	SET_VECTOR_ELT(retlist, 2, stmt_type);

	SEXP col_names = cpp_str_to_strsexp(stmtholder->stmt->names);
	SET_VECTOR_ELT(retlist, 3, col_names);

	vector<string> rtypes;

	for (auto &stype : stmtholder->stmt->types) {
		string rtype = "";
		switch (stype.id()) {
		case LogicalTypeId::BOOLEAN:
			rtype = "logical";
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			rtype = "integer";
			break;
		case LogicalTypeId::TIMESTAMP:
			rtype = "POSIXct";
			break;
		case LogicalTypeId::DATE:
			rtype = "Date";
			break;
		case LogicalTypeId::TIME:
			rtype = "difftime";
			break;
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			rtype = "numeric";
			break;
		case LogicalTypeId::VARCHAR: {
			rtype = "character";
			break;
		}
		default:
			UNPROTECT(1); // retlist
			Rf_error("duckdb_prepare_R: Unknown column type for prepare: %s", stype.ToString().c_str());
			break;
		}
		rtypes.push_back(rtype);
	}

	SEXP rtypessexp = cpp_str_to_strsexp(rtypes);
	SET_VECTOR_ELT(retlist, 4, rtypessexp);

	SET_VECTOR_ELT(retlist, 5, ScalarInteger(stmtholder->stmt->n_param));

	UNPROTECT(1); // retlist
	return retlist;
}

SEXP duckdb_bind_R(SEXP stmtsexp, SEXP paramsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_bind_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (!stmtholder || !stmtholder->stmt) {
		Rf_error("duckdb_bind_R: Invalid statement");
	}

	stmtholder->parameters.clear();
	stmtholder->parameters.resize(stmtholder->stmt->n_param);

	if (stmtholder->stmt->n_param == 0) {
		Rf_error("duckdb_bind_R: dbBind called but query takes no parameters");
		return R_NilValue;
	}

	if (TYPEOF(paramsexp) != VECSXP || (idx_t)LENGTH(paramsexp) != stmtholder->stmt->n_param) {
		Rf_error("duckdb_bind_R: bind parameters need to be a list of length %i", stmtholder->stmt->n_param);
	}

	for (idx_t param_idx = 0; param_idx < (idx_t)LENGTH(paramsexp); param_idx++) {
		Value val;
		SEXP valsexp = VECTOR_ELT(paramsexp, param_idx);
		if (LENGTH(valsexp) != 1) {
			Rf_error("duckdb_bind_R: bind parameter values need to have length 1");
		}
		auto rtype = detect_rtype(valsexp);
		switch (rtype) {
		case RType::LOGICAL: {
			auto lgl_val = INTEGER_POINTER(valsexp)[0];
			val = Value::BOOLEAN(lgl_val);
			val.is_null = RBooleanType::IsNull(lgl_val);
			break;
		}
		case RType::INTEGER: {
			auto int_val = INTEGER_POINTER(valsexp)[0];
			val = Value::INTEGER(int_val);
			val.is_null = RIntegerType::IsNull(int_val);
			break;
		}
		case RType::NUMERIC: {
			auto dbl_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::DOUBLE(dbl_val);
			val.is_null = RDoubleType::IsNull(dbl_val);
			break;
		}
		case RType::STRING: {
			auto str_val = STRING_ELT(valsexp, 0);
			val = Value(CHAR(str_val));
			val.is_null = str_val == NA_STRING;
			break;
		}
		case RType::FACTOR: {
			auto int_val = INTEGER_POINTER(valsexp)[0];
			auto levels = GET_LEVELS(valsexp);
			bool is_null = RIntegerType::IsNull(int_val);
			if (!is_null) {
				auto str_val = STRING_ELT(levels, int_val - 1);
				val = Value(CHAR(str_val));
			} else {
				val = Value(LogicalType::VARCHAR);
			}
			break;
		}
		case RType::TIMESTAMP: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIMESTAMP(RTimestampType::Convert(ts_val));
			val.is_null = RTimestampType::IsNull(ts_val);
			break;
		}
		case RType::DATE: {
			auto d_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::DATE(RDateType::Convert(d_val));
			val.is_null = RDateType::IsNull(d_val);
			break;
		}
		default:
			Rf_error("duckdb_bind_R: Unsupported parameter type");
		}
		stmtholder->parameters[param_idx] = val;
	}
	return R_NilValue;
}

SEXP duckdb_execute_R(SEXP stmtsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_execute_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (!stmtholder || !stmtholder->stmt) {
		Rf_error("duckdb_execute_R: Invalid statement");
	}

	auto generic_result = stmtholder->stmt->Execute(stmtholder->parameters, false);

	if (!generic_result->success) {
		Rf_error("duckdb_execute_R: Failed to run query\nError: %s", generic_result->error.c_str());
	}
	assert(generic_result->type == QueryResultType::MATERIALIZED_RESULT);
	MaterializedQueryResult *result = (MaterializedQueryResult *)generic_result.get();

	// step 2: create result data frame and allocate columns
	uint32_t ncols = result->types.size();
	uint64_t nrows = result->collection.count;

	if (ncols > 0) {
		SEXP retlist = PROTECT(NEW_LIST(ncols));
		SET_NAMES(retlist, cpp_str_to_strsexp(result->names));

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			SEXP varvalue = NULL;
			switch (result->types[col_idx].id()) {
			case LogicalTypeId::BOOLEAN:
				varvalue = PROTECT(NEW_LOGICAL(nrows));
				break;
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::INTEGER:
				varvalue = PROTECT(NEW_INTEGER(nrows));
				break;
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::HUGEINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
			case LogicalTypeId::DECIMAL:
			case LogicalTypeId::TIMESTAMP:
			case LogicalTypeId::DATE:
			case LogicalTypeId::TIME:
				varvalue = PROTECT(NEW_NUMERIC(nrows));
				break;
			case LogicalTypeId::VARCHAR:
				varvalue = PROTECT(NEW_STRING(nrows));
				break;
			default:
				UNPROTECT(1); // retlist
				Rf_error("duckdb_execute_R: Unknown column type for execute: %s",
				         result->types[col_idx].ToString().c_str());
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
				switch (result->types[col_idx].id()) {
				case LogicalTypeId::BOOLEAN:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], chunk->size(), LOGICAL_POINTER(dest),
					                              dest_offset, NA_LOGICAL);
					break;
				case LogicalTypeId::TINYINT:
					vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest),
					                              dest_offset, NA_INTEGER);
					break;
				case LogicalTypeId::SMALLINT:
					vector_to_r<int16_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest),
					                               dest_offset, NA_INTEGER);
					break;
				case LogicalTypeId::INTEGER:
					vector_to_r<int32_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest),
					                               dest_offset, NA_INTEGER);
					break;
				case LogicalTypeId::TIMESTAMP: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = FlatVector::GetData<int64_t>(src_vec);
					auto &nullmask = FlatVector::Nullmask(src_vec);
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
						dest_ptr[row_idx] =
						    nullmask[row_idx] ? NA_REAL : (double)Timestamp::GetEpoch(src_data[row_idx]);
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
				case LogicalTypeId::DATE: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = FlatVector::GetData<int32_t>(src_vec);
					auto &nullmask = FlatVector::Nullmask(src_vec);
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
						dest_ptr[row_idx] = nullmask[row_idx] ? NA_REAL : (double)(src_data[row_idx]) - 719528;
					}

					// some dresssup for R
					SET_CLASS(dest, PROTECT(mkString("Date")));
					UNPROTECT(1);
					break;
				}
				case LogicalTypeId::TIME: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = FlatVector::GetData<int32_t>(src_vec);
					auto &nullmask = FlatVector::Nullmask(src_vec);
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {

						if (nullmask[row_idx]) {
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
				case LogicalTypeId::BIGINT:
					vector_to_r<int64_t, double>(chunk->data[col_idx], chunk->size(), NUMERIC_POINTER(dest),
					                             dest_offset, NA_REAL);
					break;
				case LogicalTypeId::HUGEINT: {
					auto &src_vec = chunk->data[col_idx];
					auto src_data = FlatVector::GetData<hugeint_t>(src_vec);
					auto &nullmask = FlatVector::Nullmask(src_vec);
					double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
					for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
						if (nullmask[row_idx]) {
							dest_ptr[row_idx] = NA_REAL;
						} else {
							Hugeint::TryCast(src_data[row_idx], dest_ptr[row_idx]);
						}
					}
					break;
				}
				case LogicalTypeId::FLOAT:
					vector_to_r<float, double>(chunk->data[col_idx], chunk->size(), NUMERIC_POINTER(dest), dest_offset,
					                           NA_REAL);
					break;

				case LogicalTypeId::DOUBLE:
					vector_to_r<double, double>(chunk->data[col_idx], chunk->size(), NUMERIC_POINTER(dest), dest_offset,
					                            NA_REAL);
					break;
				case LogicalTypeId::VARCHAR: {
					auto src_ptr = FlatVector::GetData<string_t>(chunk->data[col_idx]);
					auto &nullmask = FlatVector::Nullmask(chunk->data[col_idx]);
					for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
						if (nullmask[row_idx]) {
							SET_STRING_ELT(dest, dest_offset + row_idx, NA_STRING);
						} else {
							SET_STRING_ELT(dest, dest_offset + row_idx, mkCharCE(src_ptr[row_idx].GetData(), CE_UTF8));
						}
					}
					break;
				}
				default:
					Rf_error("duckdb_execute_R: Unknown column type for convert: %s",
					         chunk->GetTypes()[col_idx].ToString().c_str());
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

struct DataFrameScanFunctionData : public TableFunctionData {
	DataFrameScanFunctionData(SEXP df, idx_t row_count, vector<RType> rtypes)
	    : df(df), row_count(row_count), rtypes(rtypes), position(0) {
	}
	SEXP df;
	idx_t row_count;
	vector<RType> rtypes;
	idx_t position;
};

struct DataFrameScanFunction : public TableFunction {
	DataFrameScanFunction()
	    : TableFunction("dataframe_scan", {LogicalType::VARCHAR}, dataframe_scan_bind, dataframe_scan_function,
	                    nullptr){};

	static unique_ptr<FunctionData> dataframe_scan_bind(ClientContext &context, vector<Value> &inputs,
	                                                    unordered_map<string, Value> &named_parameters,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		// TODO have a better way to pass this pointer
		SEXP df((SEXP)std::stoull(inputs[0].GetValue<string>(), nullptr, 16));

		auto df_names = GET_NAMES(df);
		vector<RType> rtypes;

		for (idx_t col_idx = 0; col_idx < (idx_t)LENGTH(df); col_idx++) {
			names.push_back(string(CHAR(STRING_ELT(df_names, col_idx))));
			SEXP coldata = VECTOR_ELT(df, col_idx);
			rtypes.push_back(detect_rtype(coldata));
			LogicalType duckdb_col_type;
			switch (rtypes[col_idx]) {
			case RType::LOGICAL:
				duckdb_col_type = LogicalType::BOOLEAN;
				break;
			case RType::INTEGER:
				duckdb_col_type = LogicalType::INTEGER;
				break;
			case RType::NUMERIC:
				duckdb_col_type = LogicalType::DOUBLE;
				break;
			case RType::FACTOR:
			case RType::STRING:
				duckdb_col_type = LogicalType::VARCHAR;
				break;
			case RType::TIMESTAMP:
				duckdb_col_type = LogicalType::TIMESTAMP;
				break;
			case RType::DATE:
				duckdb_col_type = LogicalType::DATE;
				break;
			default:
				Rf_error("Unsupported column type for scan");
			}
			return_types.push_back(duckdb_col_type);
		}

		auto row_count = LENGTH(VECTOR_ELT(df, 0));
		return make_unique<DataFrameScanFunctionData>(df, row_count, rtypes);
	}

	static void dataframe_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output,
	                                    FunctionData *dataptr) {
		auto &data = *((DataFrameScanFunctionData *)dataptr);

		if (data.position >= data.row_count) {
			return;
		}
		idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.row_count - data.position);

		output.SetCardinality(this_count);

		// TODO this is quite similar to append, unify!
		for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			auto &v = output.data[col_idx];
			SEXP coldata = VECTOR_ELT(data.df, col_idx);

			switch (data.rtypes[col_idx]) {
			case RType::LOGICAL: {
				auto data_ptr = INTEGER_POINTER(coldata) + data.position;
				AppendColumnSegment<int, bool, RBooleanType>(data_ptr, v, this_count);
				break;
			}
			case RType::INTEGER: {
				auto data_ptr = INTEGER_POINTER(coldata) + data.position;
				AppendColumnSegment<int, int, RIntegerType>(data_ptr, v, this_count);
				break;
			}
			case RType::NUMERIC: {
				auto data_ptr = NUMERIC_POINTER(coldata) + data.position;
				AppendColumnSegment<double, double, RDoubleType>(data_ptr, v, this_count);
				break;
			}
			case RType::STRING:
				AppendStringSegment(coldata, v, data.position, this_count);
				break;
			case RType::FACTOR:
				AppendFactor(coldata, v, data.position, this_count);
				break;
			case RType::TIMESTAMP: {
				auto data_ptr = NUMERIC_POINTER(coldata) + data.position;
				AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, v, this_count);
				break;
			}
			case RType::DATE: {
				auto data_ptr = NUMERIC_POINTER(coldata) + data.position;
				AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
				break;
			}
			default:
				throw;
			}
		}

		data.position += this_count;
	}
};

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
	dbaddr->LoadExtension<ParquetExtension>();

	DataFrameScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	Connection conn(*dbaddr);
	auto &context = *conn.context;
	context.transaction.BeginTransaction();
	context.catalog.CreateTableFunction(context, &info);
	context.transaction.Commit();

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

SEXP duckdb_register_R(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {

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

	if (TYPEOF(valuesexp) != VECSXP || LENGTH(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		Rf_error("duckdb_append_R: Need at least one-column data frame parameter for value");
	}

	auto key = install(("_registered_df_" + name).c_str());
	setAttrib(connsexp, key, valuesexp);

	// TODO put it into a conn attr that contains a named list to keep from gc!
	std::ostringstream address;
	address << (void const *)valuesexp;
	string pointer_str = address.str();

	// hack alert: put the pointer address into the function call as a string
	auto res = conn->Query("CREATE OR REPLACE TEMPORARY VIEW \"" + name + "\" AS SELECT * FROM dataframe_scan('" +
	                       pointer_str + "')");
	if (!res->success) {
		Rf_error(res->error.c_str());
	}
	return R_NilValue;
}

SEXP duckdb_unregister_R(SEXP connsexp, SEXP namesexp) {

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

	auto key = install(("_registered_df_" + name).c_str());
	setAttrib(connsexp, key, R_NilValue);

	auto res = conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		Rf_error(res->error.c_str());
	}

	// TODO
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

	SEXP connsexp = PROTECT(R_MakeExternalPtr(new Connection(*dbaddr), R_NilValue, R_NilValue));
	R_RegisterCFinalizer(connsexp, (void (*)(SEXP))duckdb_finalize_connection_R);
	UNPROTECT(1);

	return connsexp;
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
                                            CALLDEF(duckdb_prepare_R, 2),
                                            CALLDEF(duckdb_bind_R, 2),
                                            CALLDEF(duckdb_execute_R, 1),
                                            CALLDEF(duckdb_release_R, 1),
                                            CALLDEF(duckdb_register_R, 3),
                                            CALLDEF(duckdb_unregister_R, 2),
                                            CALLDEF(duckdb_disconnect_R, 1),
                                            CALLDEF(duckdb_shutdown_R, 1),
                                            CALLDEF(duckdb_ptr_to_str, 1),

                                            {NULL, NULL, 0}};

void R_init_duckdb(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);
}
}
