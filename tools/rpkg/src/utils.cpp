#include "rapi.hpp"
#include "typesr.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb;

SEXP RApi::PointerToString(SEXP extptr) {
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

static SEXP cstr_to_charsexp(const char *s) {
	return Rf_mkCharCE(s, CE_UTF8);
}

static SEXP cpp_str_to_charsexp(string s) {
	return cstr_to_charsexp(s.c_str());
}

SEXP RApi::StringsToSexp(vector<string> s) {
	RProtector r;
	SEXP retsexp = r.Protect(NEW_STRING(s.size()));
	for (idx_t i = 0; i < s.size(); i++) {
		SET_STRING_ELT(retsexp, i, cpp_str_to_charsexp(s[i]));
	}
	return retsexp;
}

RStrings::RStrings() {
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

Value RApiTypes::SexpToValue(SEXP valsexp, R_len_t idx) {
	Value val;
	auto rtype = RApiTypes::DetectRType(valsexp);
	switch (rtype) {
	case RType::LOGICAL: {
		auto lgl_val = INTEGER_POINTER(valsexp)[idx];
		val = Value::BOOLEAN(lgl_val);
		val.is_null = RBooleanType::IsNull(lgl_val);
		break;
	}
	case RType::INTEGER: {
		auto int_val = INTEGER_POINTER(valsexp)[idx];
		val = Value::INTEGER(int_val);
		val.is_null = RIntegerType::IsNull(int_val);
		break;
	}
	case RType::NUMERIC: {
		auto dbl_val = NUMERIC_POINTER(valsexp)[idx];
		bool is_null = RDoubleType::IsNull(dbl_val);
		if (is_null) {
			val = Value(LogicalType::DOUBLE);
		} else {
			val = Value::DOUBLE(dbl_val);
		}
		break;
	}
	case RType::STRING: {
		auto str_val = STRING_ELT(valsexp, idx);
		val = Value(CHAR(str_val));
		val.is_null = str_val == NA_STRING;
		break;
	}
	case RType::FACTOR: {
		auto int_val = INTEGER_POINTER(valsexp)[idx];
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
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIMESTAMP(RTimestampType::Convert(ts_val));
		val.is_null = RTimestampType::IsNull(ts_val);
		break;
	}
	case RType::DATE: {
		auto d_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::DATE(RDateType::Convert(d_val));
		val.is_null = RDateType::IsNull(d_val);
		break;
	}
	case RType::TIME_SECONDS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIME(RTimeSecondsType::Convert(ts_val));
		val.is_null = RTimeSecondsType::IsNull(ts_val);
		break;
	}
	case RType::TIME_MINUTES: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIME(RTimeMinutesType::Convert(ts_val));
		val.is_null = RTimeMinutesType::IsNull(ts_val);
		break;
	}
	case RType::TIME_HOURS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIME(RTimeHoursType::Convert(ts_val));
		val.is_null = RTimeHoursType::IsNull(ts_val);
		break;
	}
	case RType::TIME_DAYS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIME(RTimeDaysType::Convert(ts_val));
		val.is_null = RTimeDaysType::IsNull(ts_val);
		break;
	}
	case RType::TIME_WEEKS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		val = Value::TIME(RTimeWeeksType::Convert(ts_val));
		val.is_null = RTimeWeeksType::IsNull(ts_val);
		break;
	}
	default:
		Rf_error("duckdb_sexp_to_value: Unsupported type");
	}
	return val;
}

SEXP RApiTypes::ValueToSexp(Value &val) {
	if (val.is_null) {
		return R_NilValue;
	}
	RProtector r;
	SEXP res;
	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		res = r.Protect(NEW_LOGICAL(1));
		LOGICAL_POINTER(res)[0] = val.GetValue<bool>();
		return res;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		res = r.Protect(NEW_INTEGER(1));
		INTEGER_POINTER(res)[0] = val.GetValue<int32_t>();
		return res;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		res = r.Protect(NEW_NUMERIC(1));
		NUMERIC_POINTER(res)[0] = val.GetValue<double>();
		return res;
	case LogicalTypeId::VARCHAR:
		res = r.Protect(NEW_STRING(1));
		SET_STRING_ELT(res, 0, cpp_str_to_charsexp(val.ToString()));
		return res;
	case LogicalTypeId::TIMESTAMP: {
		// TODO bit of duplication here with statement.cpp, fix this
		res = r.Protect(NEW_NUMERIC(1));
		double *dest_ptr = ((double *)NUMERIC_POINTER(res));
		dest_ptr[0] = (double)Timestamp::GetEpochSeconds(val.value_.timestamp);
		// some dresssup for R
		RProtector r_ts;
		SEXP cl = r_ts.Protect(NEW_STRING(2));
		SET_STRING_ELT(cl, 0, r_ts.Protect(Rf_mkChar("POSIXct")));
		SET_STRING_ELT(cl, 1, r_ts.Protect(Rf_mkChar("POSIXt")));
		SET_CLASS(res, cl);
		Rf_setAttrib(res, Rf_install("tzone"), r_ts.Protect(Rf_mkString("UTC")));
		return res;
	}

	case LogicalTypeId::DATE: {
		res = r.Protect(NEW_NUMERIC(1));
		double *dest_ptr = ((double *)NUMERIC_POINTER(res));
		dest_ptr[0] = (double)int32_t(val.value_.date);
		// some dresssup for R
		RProtector r_date;
		SET_CLASS(res, r_date.Protect(Rf_mkString("Date")));
		return res;
	}

	default:
		throw NotImplementedException("Can't convert %s of type %s", val.ToString(), val.type().ToString());
	}
}

SEXP RApi::REvalThrows(SEXP call, SEXP env) {
	RProtector r;
	int err;
	auto res = r.Protect(R_tryEval(call, env, &err));
	if (err) {
		throw InternalException("Failed to eval R expression %s", R_curErrorBuf());
	}
	return res;
}

SEXP RApi::REvalRerror(SEXP call, SEXP env) {
	try {
		return REvalThrows(call, env);
	} catch (std::exception &e) {
		Rf_error(e.what());
	}
}
