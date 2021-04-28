#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "extension/extension_helper.hpp"

#define R_NO_REMAP

#include <Rdefines.h>
#include <R_ext/Altrep.h>

#include <algorithm>

using namespace duckdb;
using namespace std;

struct RProtector {
	RProtector() : protect_count(0) {
	}
	~RProtector() {
		if (protect_count > 0) {
			UNPROTECT(protect_count);
		}
	}

	SEXP Protect(SEXP sexp) {
		protect_count++;
		return PROTECT(sexp);
	}

private:
	int protect_count;
};

struct RStrings {
	SEXP secs;
	SEXP mins;
	SEXP hours;
	SEXP days;
	SEXP weeks;

	static const RStrings &get() {
		// On demand
		static RStrings strings;
		return strings;
	}

private:
	RStrings() {
		// allocate strings once
		RProtector r;

		SEXP out = r.Protect(Rf_allocVector(STRSXP, 5));
		SET_STRING_ELT(out, 0, secs = Rf_mkChar("secs"));
		SET_STRING_ELT(out, 1, mins = Rf_mkChar("mins"));
		SET_STRING_ELT(out, 2, hours = Rf_mkChar("hours"));
		SET_STRING_ELT(out, 3, days = Rf_mkChar("days"));
		SET_STRING_ELT(out, 4, weeks = Rf_mkChar("weeks"));
		R_PreserveObject(out);
		MARK_NOT_MUTABLE(out);
	}
};

struct RStatement {
	unique_ptr<PreparedStatement> stmt;
	vector<Value> parameters;
};

// converter for primitive types
template <class SRC, class DEST>
static void vector_to_r(Vector &src_vec, size_t count, void *dest, uint64_t dest_offset, DEST na_val) {
	auto src_ptr = FlatVector::GetData<SRC>(src_vec);
	auto &mask = FlatVector::Validity(src_vec);
	auto dest_ptr = ((DEST *)dest) + dest_offset;
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] = !mask.RowIsValid(row_idx) ? na_val : src_ptr[row_idx];
	}
}

struct RIntegralType {
	template <class T>
	static double DoubleCast(T val) {
		return double(val);
	}
};

template <class T>
static void RDecimalCastLoop(Vector &src_vec, size_t count, double *dest_ptr, uint8_t scale) {
	auto src_ptr = FlatVector::GetData<T>(src_vec);
	auto &mask = FlatVector::Validity(src_vec);
	double division = pow(10, scale);
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] =
		    !mask.RowIsValid(row_idx) ? NA_REAL : RIntegralType::DoubleCast<T>(src_ptr[row_idx]) / division;
	}
}

template <>
double RIntegralType::DoubleCast<>(hugeint_t val) {
	return Hugeint::Cast<double>(val);
}

struct RDoubleType {
	static bool IsNull(double val) {
		return ISNA(val);
	}

	static double Convert(double val) {
		return val;
	}
};

struct RDateType : public RDoubleType {
	static date_t Convert(double val) {
		return date_t((int32_t)val);
	}
};

struct RTimestampType : public RDoubleType {
	static timestamp_t Convert(double val) {
		return Timestamp::FromEpochSeconds(val);
	}
};

struct RTimeSecondsType : public RDoubleType {
	static dtime_t Convert(double val) {
		return dtime_t(int64_t(val * Interval::MICROS_PER_SEC));
	}
};

struct RTimeMinutesType : public RDoubleType {
	static dtime_t Convert(double val) {
		return dtime_t(int64_t(val * Interval::MICROS_PER_MINUTE));
	}
};

struct RTimeHoursType : public RDoubleType {
	static dtime_t Convert(double val) {
		return dtime_t(int64_t(val * Interval::MICROS_PER_HOUR));
	}
};

struct RTimeDaysType : public RDoubleType {
	static dtime_t Convert(double val) {
		return dtime_t(int64_t(val * Interval::MICROS_PER_DAY));
	}
};

struct RTimeWeeksType : public RDoubleType {
	static dtime_t Convert(double val) {
		return dtime_t(int64_t(val * Interval::MICROS_PER_DAY * 7));
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

struct RBooleanType : public RIntegerType {
	static bool Convert(int val) {
		return val;
	}
};

template <class SRC, class DST, class RTYPE>
static void AppendColumnSegment(SRC *source_data, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<DST>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto val = source_data[i];
		if (RTYPE::IsNull(val)) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = RTYPE::Convert(val);
		}
	}
}

static void AppendStringSegment(SEXP coldata, Vector &result, idx_t row_idx, idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		SEXP val = STRING_ELT(coldata, row_idx + i);
		if (val == NA_STRING) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = string_t((char *)CHAR(val));
		}
	}
}

static void AppendFactor(SEXP coldata, Vector &result, idx_t row_idx, idx_t count) {
	auto source_data = INTEGER_POINTER(coldata) + row_idx;
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	SEXP factor_levels = GET_LEVELS(coldata);
	for (idx_t i = 0; i < count; i++) {
		int val = source_data[i];
		if (RIntegerType::IsNull(val)) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = string_t(CHAR(STRING_ELT(factor_levels, val - 1)));
		}
	}
}

static SEXP cstr_to_charsexp(const char *s) {
	return Rf_mkCharCE(s, CE_UTF8);
}

static SEXP cpp_str_to_charsexp(string s) {
	return cstr_to_charsexp(s.c_str());
}

static SEXP cpp_str_to_strsexp(vector<string> s) {
	RProtector r;
	SEXP retsexp = r.Protect(NEW_STRING(s.size()));
	for (idx_t i = 0; i < s.size(); i++) {
		SET_STRING_ELT(retsexp, i, cpp_str_to_charsexp(s[i]));
	}
	return retsexp;
}

enum class RType {
	UNKNOWN,
	LOGICAL,
	INTEGER,
	NUMERIC,
	STRING,
	FACTOR,
	DATE,
	TIMESTAMP,
	TIME_SECONDS,
	TIME_MINUTES,
	TIME_HOURS,
	TIME_DAYS,
	TIME_WEEKS
};

static RType detect_rtype(SEXP v) {
	if (TYPEOF(v) == REALSXP && Rf_inherits(v, "POSIXct")) {
		return RType::TIMESTAMP;
	} else if (TYPEOF(v) == REALSXP && Rf_inherits(v, "Date")) {
		return RType::DATE;
	} else if (TYPEOF(v) == REALSXP && Rf_inherits(v, "difftime")) {
		SEXP units = Rf_getAttrib(v, Rf_install("units"));
		if (TYPEOF(units) != STRSXP) {
			return RType::UNKNOWN;
		}
		SEXP units0 = STRING_ELT(units, 0);
		if (units0 == RStrings::get().secs) {
			return RType::TIME_SECONDS;
		} else if (units0 == RStrings::get().mins) {
			return RType::TIME_MINUTES;
		} else if (units0 == RStrings::get().hours) {
			return RType::TIME_HOURS;
		} else if (units0 == RStrings::get().days) {
			return RType::TIME_DAYS;
		} else if (units0 == RStrings::get().weeks) {
			return RType::TIME_WEEKS;
		} else {
			return RType::UNKNOWN;
		}
	} else if (Rf_isFactor(v) && TYPEOF(v) == INTSXP) {
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

static R_altrep_class_t duckdb_altrep_string_class;

struct DuckDBAltrepStringWrapper {
	vector<Vector> vectors;
	idx_t length;
};

static DuckDBAltrepStringWrapper *duckdb_altrep_wrapper(SEXP x) {
	auto wrapper = (DuckDBAltrepStringWrapper *)R_ExternalPtrAddr(R_altrep_data1(x));
	if (!wrapper) {
		Rf_error("This looks like it has been freed");
	}
	return wrapper;
}

static R_xlen_t duckdb_altrep_strings_length(SEXP x) {
	return duckdb_altrep_wrapper(x)->length;
}

static Rboolean duckdb_altrep_strings_inspect(SEXP x, int pre, int deep, int pvec,
                                              void (*inspect_subtree)(SEXP, int, int, int)) {
	Rprintf("DUCKDB_STRING_COLUMN %llu\n", duckdb_altrep_strings_length(x));
	return TRUE;
}

static void *duckdb_altrep_strings_dataptr(SEXP x, Rboolean writeable) {
	auto *wrapper = duckdb_altrep_wrapper(x);
	if (R_altrep_data2(x) == R_NilValue) {
		R_set_altrep_data2(x, NEW_STRING(wrapper->length));
		idx_t dest_offset = 0;
		for (auto &vec : wrapper->vectors) {
			auto src_ptr = FlatVector::GetData<string_t>(vec);
			auto &mask = FlatVector::Validity(vec);
			for (size_t row_idx = 0; row_idx < MinValue<idx_t>(STANDARD_VECTOR_SIZE, wrapper->length - dest_offset);
			     row_idx++) {
				if (!mask.RowIsValid(row_idx)) {
					SET_STRING_ELT(R_altrep_data2(x), dest_offset + row_idx, NA_STRING);
				} else {
					SET_STRING_ELT(
					    R_altrep_data2(x), dest_offset + row_idx,
					    Rf_mkCharLenCE(src_ptr[row_idx].GetDataUnsafe(), src_ptr[row_idx].GetSize(), CE_UTF8));
				}
			}
			dest_offset += STANDARD_VECTOR_SIZE;
		}
		wrapper->vectors.clear();
	}
	return CHARACTER_POINTER(R_altrep_data2(x));
}

static const void *duckdb_altrep_strings_dataptr_or_null(SEXP x) {
	return nullptr;
}

static SEXP duckdb_altrep_strings_elt(SEXP x, R_xlen_t i) {
	auto *wrapper = duckdb_altrep_wrapper(x);
	if (R_altrep_data2(x) != R_NilValue) {
		return STRING_ELT(R_altrep_data2(x), i);
	}
	auto &vec = wrapper->vectors[i / STANDARD_VECTOR_SIZE];
	auto src_ptr = FlatVector::GetData<string_t>(vec);
	auto &mask = FlatVector::Validity(vec);
	auto vec_idx = i % STANDARD_VECTOR_SIZE;
	if (!mask.RowIsValid(vec_idx)) {
		return NA_STRING;
	}
	return Rf_mkCharLenCE(src_ptr[vec_idx].GetDataUnsafe(), src_ptr[vec_idx].GetSize(), CE_UTF8);
}

static void duckdb_altrep_strings_set_elt(SEXP x, R_xlen_t i, SEXP val) {
	duckdb_altrep_strings_dataptr(x, TRUE);
	SET_STRING_ELT(R_altrep_data2(x), i, val);
}

static int duckdb_altrep_strings_is_sorted(SEXP x) {
	// we don't know
	return 0;
}

static int duckdb_altrep_strings_no_na(SEXP x) {
	// we kinda know but it matters little
	return 0;
}

static void duckdb_altrep_strings_finalizer(SEXP x) {
	auto *wrapper = (DuckDBAltrepStringWrapper *)R_ExternalPtrAddr(x);
	if (wrapper) {
		R_ClearExternalPtr(x);
		delete wrapper;
	}
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
	RProtector r;
	if (TYPEOF(querysexp) != STRSXP || Rf_length(querysexp) != 1) {
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

	SEXP retlist = r.Protect(NEW_LIST(6));

	SEXP stmtsexp = r.Protect(R_MakeExternalPtr(stmtholder, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(stmtsexp, (void (*)(SEXP))duckdb_finalize_statement_R);

	SEXP ret_names = cpp_str_to_strsexp({"str", "ref", "type", "names", "rtypes", "n_param"});
	SET_NAMES(retlist, ret_names);

	SET_VECTOR_ELT(retlist, 0, querysexp);
	SET_VECTOR_ELT(retlist, 1, stmtsexp);

	SEXP stmt_type = cpp_str_to_strsexp({StatementTypeToString(stmtholder->stmt->GetStatementType())});
	SET_VECTOR_ELT(retlist, 2, stmt_type);

	SEXP col_names = cpp_str_to_strsexp(stmtholder->stmt->GetNames());
	SET_VECTOR_ELT(retlist, 3, col_names);

	vector<string> rtypes;

	for (auto &stype : stmtholder->stmt->GetTypes()) {
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
		case LogicalTypeId::DECIMAL:
			rtype = "numeric";
			break;
		case LogicalTypeId::VARCHAR:
			rtype = "character";
			break;
		case LogicalTypeId::BLOB:
			rtype = "raw";
			break;
		default:
			Rf_error("duckdb_prepare_R: Unknown column type for prepare: %s", stype.ToString().c_str());
			break;
		}
		rtypes.push_back(rtype);
	}

	SEXP rtypessexp = cpp_str_to_strsexp(rtypes);
	SET_VECTOR_ELT(retlist, 4, rtypessexp);

	SET_VECTOR_ELT(retlist, 5, Rf_ScalarInteger(stmtholder->stmt->n_param));

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

	if (TYPEOF(paramsexp) != VECSXP || (idx_t)Rf_length(paramsexp) != stmtholder->stmt->n_param) {
		Rf_error("duckdb_bind_R: bind parameters need to be a list of length %i", stmtholder->stmt->n_param);
	}

	for (idx_t param_idx = 0; param_idx < (idx_t)Rf_length(paramsexp); param_idx++) {
		Value val;
		SEXP valsexp = VECTOR_ELT(paramsexp, param_idx);
		if (Rf_length(valsexp) != 1) {
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
		case RType::TIME_SECONDS: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIME(RTimeSecondsType::Convert(ts_val));
			val.is_null = RTimeSecondsType::IsNull(ts_val);
			break;
		}
		case RType::TIME_MINUTES: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIME(RTimeMinutesType::Convert(ts_val));
			val.is_null = RTimeMinutesType::IsNull(ts_val);
			break;
		}
		case RType::TIME_HOURS: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIME(RTimeHoursType::Convert(ts_val));
			val.is_null = RTimeHoursType::IsNull(ts_val);
			break;
		}
		case RType::TIME_DAYS: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIME(RTimeDaysType::Convert(ts_val));
			val.is_null = RTimeDaysType::IsNull(ts_val);
			break;
		}
		case RType::TIME_WEEKS: {
			auto ts_val = NUMERIC_POINTER(valsexp)[0];
			val = Value::TIME(RTimeWeeksType::Convert(ts_val));
			val.is_null = RTimeWeeksType::IsNull(ts_val);
			break;
		}
		default:
			Rf_error("duckdb_bind_R: Unsupported parameter type");
		}
		stmtholder->parameters[param_idx] = val;
	}
	return R_NilValue;
}

SEXP duckdb_execute_R_impl(MaterializedQueryResult *result);

SEXP duckdb_execute_R(SEXP stmtsexp) {
	if (TYPEOF(stmtsexp) != EXTPTRSXP) {
		Rf_error("duckdb_execute_R: Need external pointer parameter");
	}
	RStatement *stmtholder = (RStatement *)R_ExternalPtrAddr(stmtsexp);
	if (!stmtholder || !stmtholder->stmt) {
		Rf_error("duckdb_execute_R: Invalid statement");
	}

	RProtector r;
	SEXP out;

	{
		auto generic_result = stmtholder->stmt->Execute(stmtholder->parameters, false);

		if (!generic_result->success) {
			Rf_error("duckdb_execute_R: Failed to run query\nError: %s", generic_result->error.c_str());
		}
		D_ASSERT(generic_result->type == QueryResultType::MATERIALIZED_RESULT);
		MaterializedQueryResult *result = (MaterializedQueryResult *)generic_result.get();

		// Protect during destruction of generic_result
		out = r.Protect(duckdb_execute_R_impl(result));
	}

	return out;
}

SEXP duckdb_execute_R_impl(MaterializedQueryResult *result) {
	RProtector r;
	// step 2: create result data frame and allocate columns
	uint32_t ncols = result->types.size();
	if (ncols == 0) {
		return Rf_ScalarReal(0); // no need for protection because no allocation can happen afterwards
	}

	uint64_t nrows = result->collection.Count();
	SEXP retlist = r.Protect(NEW_LIST(ncols));
	SET_NAMES(retlist, cpp_str_to_strsexp(result->names));

	for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
		RProtector r_varvalue;

		SEXP varvalue = NULL;
		switch (result->types[col_idx].id()) {
		case LogicalTypeId::BOOLEAN:
			varvalue = r_varvalue.Protect(NEW_LOGICAL(nrows));
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			varvalue = r_varvalue.Protect(NEW_INTEGER(nrows));
			break;
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
			varvalue = r_varvalue.Protect(NEW_NUMERIC(nrows));
			break;
		case LogicalTypeId::VARCHAR: {
			auto wrapper = new DuckDBAltrepStringWrapper();
			wrapper->length = nrows;
			wrapper->vectors.resize(result->collection.Chunks().size());

			auto ptr = PROTECT(R_MakeExternalPtr((void *)wrapper, R_NilValue, R_NilValue));
			R_RegisterCFinalizer(ptr, duckdb_altrep_strings_finalizer);
			varvalue = r_varvalue.Protect(R_new_altrep(duckdb_altrep_string_class, ptr, R_NilValue));
			UNPROTECT(1);
			break;
		}

		case LogicalTypeId::BLOB:
			varvalue = r_varvalue.Protect(NEW_LIST(nrows));
			break;
		default:
			Rf_error("duckdb_execute_R: Unknown column type for execute: %s",
			         result->types[col_idx].ToString().c_str());
		}
		if (!varvalue) {
			throw std::bad_alloc();
		}
		SET_VECTOR_ELT(retlist, col_idx, varvalue);
	}

	// at this point retlist is fully allocated and the only protected SEXP

	// step 3: set values from chunks
	uint64_t dest_offset = 0;
	idx_t chunk_idx = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		D_ASSERT(chunk->ColumnCount() == ncols);
		D_ASSERT(chunk->ColumnCount() == (idx_t)Rf_length(retlist));
		for (size_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
			SEXP dest = VECTOR_ELT(retlist, col_idx);
			switch (result->types[col_idx].id()) {
			case LogicalTypeId::BOOLEAN:
				vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], chunk->size(), LOGICAL_POINTER(dest), dest_offset,
				                              NA_LOGICAL);
				break;
			case LogicalTypeId::TINYINT:
				vector_to_r<int8_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest), dest_offset,
				                              NA_INTEGER);
				break;
			case LogicalTypeId::SMALLINT:
				vector_to_r<int16_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest), dest_offset,
				                               NA_INTEGER);
				break;
			case LogicalTypeId::INTEGER:
				vector_to_r<int32_t, uint32_t>(chunk->data[col_idx], chunk->size(), INTEGER_POINTER(dest), dest_offset,
				                               NA_INTEGER);
				break;
			case LogicalTypeId::TIMESTAMP: {
				auto &src_vec = chunk->data[col_idx];
				auto src_data = FlatVector::GetData<timestamp_t>(src_vec);
				auto &mask = FlatVector::Validity(src_vec);
				double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
				for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					dest_ptr[row_idx] =
					    !mask.RowIsValid(row_idx) ? NA_REAL : (double)Timestamp::GetEpochSeconds(src_data[row_idx]);
				}

				// some dresssup for R
				RProtector r_ts;
				SEXP cl = r_ts.Protect(NEW_STRING(2));
				SET_STRING_ELT(cl, 0, r_ts.Protect(Rf_mkChar("POSIXct")));
				SET_STRING_ELT(cl, 1, r_ts.Protect(Rf_mkChar("POSIXt")));
				SET_CLASS(dest, cl);
				Rf_setAttrib(dest, Rf_install("tzone"), r_ts.Protect(Rf_mkString("UTC")));
				break;
			}
			case LogicalTypeId::DATE: {
				auto &src_vec = chunk->data[col_idx];
				auto src_data = FlatVector::GetData<date_t>(src_vec);
				auto &mask = FlatVector::Validity(src_vec);
				double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
				for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					dest_ptr[row_idx] = !mask.RowIsValid(row_idx) ? NA_REAL : (double)int32_t(src_data[row_idx]);
				}

				// some dresssup for R
				RProtector r_date;
				SET_CLASS(dest, r_date.Protect(Rf_mkString("Date")));
				break;
			}
			case LogicalTypeId::TIME: {
				auto &src_vec = chunk->data[col_idx];
				auto src_data = FlatVector::GetData<dtime_t>(src_vec);
				auto &mask = FlatVector::Validity(src_vec);
				double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
				for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					if (!mask.RowIsValid(row_idx)) {
						dest_ptr[row_idx] = NA_REAL;
					} else {
						dtime_t n = src_data[row_idx];
						dest_ptr[row_idx] = n.micros / 1000000.0;
					}
				}

				// some dresssup for R
				RProtector r_time;
				SET_CLASS(dest, r_time.Protect(Rf_mkString("difftime")));
				Rf_setAttrib(dest, Rf_install("units"), r_time.Protect(Rf_mkString("secs")));
				break;
			}
			case LogicalTypeId::BIGINT:
				vector_to_r<int64_t, double>(chunk->data[col_idx], chunk->size(), NUMERIC_POINTER(dest), dest_offset,
				                             NA_REAL);
				break;
			case LogicalTypeId::HUGEINT: {
				auto &src_vec = chunk->data[col_idx];
				auto src_data = FlatVector::GetData<hugeint_t>(src_vec);
				auto &mask = FlatVector::Validity(src_vec);
				double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
				for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					if (!mask.RowIsValid(row_idx)) {
						dest_ptr[row_idx] = NA_REAL;
					} else {
						Hugeint::TryCast(src_data[row_idx], dest_ptr[row_idx]);
					}
				}
				break;
			}
			case LogicalTypeId::DECIMAL: {
				auto &src_vec = chunk->data[col_idx];
				auto &decimal_type = result->types[col_idx];
				double *dest_ptr = ((double *)NUMERIC_POINTER(dest)) + dest_offset;
				auto dec_scale = decimal_type.scale();
				switch (decimal_type.InternalType()) {
				case PhysicalType::INT16:
					RDecimalCastLoop<int16_t>(src_vec, chunk->size(), dest_ptr, dec_scale);
					break;
				case PhysicalType::INT32:
					RDecimalCastLoop<int32_t>(src_vec, chunk->size(), dest_ptr, dec_scale);
					break;
				case PhysicalType::INT64:
					RDecimalCastLoop<int64_t>(src_vec, chunk->size(), dest_ptr, dec_scale);
					break;
				case PhysicalType::INT128:
					RDecimalCastLoop<hugeint_t>(src_vec, chunk->size(), dest_ptr, dec_scale);
					break;
				default:
					throw NotImplementedException("Unimplemented internal type for DECIMAL");
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
				auto wrapper = (DuckDBAltrepStringWrapper *)R_ExternalPtrAddr(R_altrep_data1(dest));
				wrapper->vectors[chunk_idx].Reference(chunk->data[col_idx]);
				break;
			}
			case LogicalTypeId::BLOB: {
				auto src_ptr = FlatVector::GetData<string_t>(chunk->data[col_idx]);
				auto &mask = FlatVector::Validity(chunk->data[col_idx]);
				for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					if (!mask.RowIsValid(row_idx)) {
						SET_VECTOR_ELT(dest, dest_offset + row_idx, Rf_ScalarLogical(NA_LOGICAL));
					} else {
						SEXP rawval = NEW_RAW(src_ptr[row_idx].GetSize());
						if (!rawval) {
							throw std::bad_alloc();
						}
						memcpy(RAW_POINTER(rawval), src_ptr[row_idx].GetDataUnsafe(), src_ptr[row_idx].GetSize());
						SET_VECTOR_ELT(dest, dest_offset + row_idx, rawval);
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
		chunk_idx++;
	}

	D_ASSERT(dest_offset == nrows);
	return retlist;
}

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		Rf_warning("duckdb_finalize_database_R: Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
		           "duckdb::duckdb_shutdown(drv) to avoid this.");
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}
	return R_NilValue;
}

struct DataFrameScanFunctionData : public TableFunctionData {
	DataFrameScanFunctionData(SEXP df, idx_t row_count, vector<RType> rtypes)
	    : df(df), row_count(row_count), rtypes(rtypes) {
	}
	SEXP df;
	idx_t row_count;
	vector<RType> rtypes;
};

struct DataFrameScanState : public FunctionOperatorData {
	DataFrameScanState() : position(0) {
	}

	idx_t position;
};

struct DataFrameScanFunction : public TableFunction {
	DataFrameScanFunction()
	    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, dataframe_scan_function, dataframe_scan_bind,
	                    dataframe_scan_init, nullptr, nullptr, nullptr, dataframe_scan_cardinality) {};

	static unique_ptr<FunctionData> dataframe_scan_bind(ClientContext &context, vector<Value> &inputs,
	                                                    unordered_map<string, Value> &named_parameters,
	                                                    vector<LogicalType> &input_table_types,
	                                                    vector<string> &input_table_names,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		RProtector r;
		SEXP df((SEXP)inputs[0].GetValue<uintptr_t>());

		auto df_names = r.Protect(GET_NAMES(df));
		vector<RType> rtypes;

		for (idx_t col_idx = 0; col_idx < (idx_t)Rf_length(df); col_idx++) {
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
			case RType::TIME_SECONDS:
			case RType::TIME_MINUTES:
			case RType::TIME_HOURS:
			case RType::TIME_DAYS:
			case RType::TIME_WEEKS:
				duckdb_col_type = LogicalType::TIME;
				break;
			case RType::DATE:
				duckdb_col_type = LogicalType::DATE;
				break;
			default:
				Rf_error("Unsupported column type for scan");
			}
			return_types.push_back(duckdb_col_type);
		}

		auto row_count = Rf_length(VECTOR_ELT(df, 0));
		return make_unique<DataFrameScanFunctionData>(df, row_count, rtypes);
	}

	static unique_ptr<FunctionOperatorData> dataframe_scan_init(ClientContext &context, const FunctionData *bind_data,
	                                                            vector<column_t> &column_ids,
	                                                            TableFilterCollection *filters) {
		return make_unique<DataFrameScanState>();
	}

	static void dataframe_scan_function(ClientContext &context, const FunctionData *bind_data,
	                                    FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
		auto &data = (DataFrameScanFunctionData &)*bind_data;
		auto &state = (DataFrameScanState &)*operator_state;
		if (state.position >= data.row_count) {
			return;
		}
		idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.row_count - state.position);

		output.SetCardinality(this_count);

		// TODO this is quite similar to append, unify!
		for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
			auto &v = output.data[col_idx];
			SEXP coldata = VECTOR_ELT(data.df, col_idx);

			switch (data.rtypes[col_idx]) {
			case RType::LOGICAL: {
				auto data_ptr = INTEGER_POINTER(coldata) + state.position;
				AppendColumnSegment<int, bool, RBooleanType>(data_ptr, v, this_count);
				break;
			}
			case RType::INTEGER: {
				auto data_ptr = INTEGER_POINTER(coldata) + state.position;
				AppendColumnSegment<int, int, RIntegerType>(data_ptr, v, this_count);
				break;
			}
			case RType::NUMERIC: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, double, RDoubleType>(data_ptr, v, this_count);
				break;
			}
			case RType::STRING:
				AppendStringSegment(coldata, v, state.position, this_count);
				break;
			case RType::FACTOR:
				AppendFactor(coldata, v, state.position, this_count);
				break;
			case RType::TIMESTAMP: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, v, this_count);
				break;
			}
			case RType::TIME_SECONDS: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
				break;
			}
			case RType::TIME_MINUTES: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
				break;
			}
			case RType::TIME_HOURS: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
				break;
			}
			case RType::TIME_DAYS: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
				break;
			}
			case RType::TIME_WEEKS: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
				break;
			}
			case RType::DATE: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
				break;
			}
			default:
				throw;
			}
		}

		state.position += this_count;
	}

	static unique_ptr<NodeStatistics> dataframe_scan_cardinality(ClientContext &context,
	                                                             const FunctionData *bind_data) {
		auto &data = (DataFrameScanFunctionData &)*bind_data;
		return make_unique<NodeStatistics>(data.row_count, data.row_count);
	}
};

SEXP duckdb_startup_R(SEXP dbdirsexp, SEXP readonlysexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || Rf_length(dbdirsexp) != 1) {
		Rf_error("duckdb_startup_R: Need string parameter for dbdir");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));

	if (TYPEOF(readonlysexp) != LGLSXP || Rf_length(readonlysexp) != 1) {
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
	} catch (exception &e) {
		Rf_error("duckdb_startup_R: Failed to open database: %s", e.what());
	}
	ExtensionHelper::LoadAllExtensions(*dbaddr);

	DataFrameScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	Connection conn(*dbaddr);
	auto &context = *conn.context;
	auto &catalog = Catalog::GetCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &info);
	context.transaction.Commit();

	RProtector r;

	SEXP dbsexp = r.Protect(R_MakeExternalPtr(dbaddr, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(dbsexp, (void (*)(SEXP))duckdb_finalize_database_R);
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
		Rf_warning("duckdb_finalize_connection_R: Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		delete connaddr;
	}
	return R_NilValue;
}

SEXP duckdb_register_R(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_register_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_register_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_register_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	if (name.empty()) {
		Rf_error("duckdb_register_R: name parameter cannot be empty");
	}
	if (TYPEOF(valuesexp) != VECSXP || Rf_length(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		Rf_error("duckdb_register_R: Need at least one-column data frame parameter for value");
	}
	try {
		conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)valuesexp)})->CreateView(name, true, true);
		auto key = Rf_install(("_registered_df_" + name).c_str());
		Rf_setAttrib(connsexp, key, valuesexp);
	} catch (exception &e) {
		Rf_error("duckdb_register_R: Failed to register data frame: %s", e.what());
	}
	return R_NilValue;
}

SEXP duckdb_unregister_R(SEXP connsexp, SEXP namesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_unregister_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_unregister_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_unregister_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	auto key = Rf_install(("_registered_df_" + name).c_str());
	Rf_setAttrib(connsexp, key, R_NilValue);
	auto res = conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		Rf_error(res->error.c_str());
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

	RProtector r;
	SEXP connsexp = r.Protect(R_MakeExternalPtr(new Connection(*dbaddr), R_NilValue, R_NilValue));
	R_RegisterCFinalizer(connsexp, (void (*)(SEXP))duckdb_finalize_connection_R);

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
	RProtector r;
	SEXP ret = r.Protect(NEW_STRING(1));
	SET_STRING_ELT(ret, 0, NA_STRING);
	void *ptr = R_ExternalPtrAddr(extptr);
	if (ptr != NULL) {
		char buf[100];
		snprintf(buf, 100, "%p", ptr);
		SET_STRING_ELT(ret, 0, Rf_mkChar(buf));
	}
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

	duckdb_altrep_string_class = R_make_altstring_class("duckdb_strings", "duckdb", dll);

	/* override ALTREP methods */
	R_set_altrep_Inspect_method(duckdb_altrep_string_class, duckdb_altrep_strings_inspect);
	R_set_altrep_Length_method(duckdb_altrep_string_class, duckdb_altrep_strings_length);

	/* override ALTVEC methods */
	R_set_altvec_Dataptr_method(duckdb_altrep_string_class, duckdb_altrep_strings_dataptr);
	R_set_altvec_Dataptr_or_null_method(duckdb_altrep_string_class, duckdb_altrep_strings_dataptr_or_null);

	/* override ALTSTRING methods */
	R_set_altstring_Elt_method(duckdb_altrep_string_class, duckdb_altrep_strings_elt);
	R_set_altstring_Is_sorted_method(duckdb_altrep_string_class, duckdb_altrep_strings_is_sorted);
	R_set_altstring_No_NA_method(duckdb_altrep_string_class, duckdb_altrep_strings_no_na);
	R_set_altstring_Set_elt_method(duckdb_altrep_string_class, duckdb_altrep_strings_set_elt);

	// TODO implement SEXP (*R_altvec_Extract_subset_method_t)(SEXP, SEXP, SEXP);
}
}
