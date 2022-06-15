#include "rapi.hpp"
#include "typesr.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb;

SEXP duckdb::ToUtf8(SEXP string_sexp) {
	cpp11::function enc2utf8 = RStrings::get().enc2utf8_sym;
	return enc2utf8(string_sexp);
}

[[cpp11::register]] cpp11::r_string rapi_ptr_to_str(SEXP extptr) {
	if (TYPEOF(extptr) != EXTPTRSXP) {
		cpp11::stop("rapi_ptr_to_str: Need external pointer parameter");
	}

	void *ptr = R_ExternalPtrAddr(extptr);
	if (ptr != NULL) {
		char buf[100];
		snprintf(buf, 100, "%p", ptr);
		return cpp11::r_string(buf);
	} else {
		return cpp11::r_string(NA_STRING);
	}
}

static SEXP cstr_to_charsexp(const char *s) {
	return Rf_mkCharCE(s, CE_UTF8);
}

static SEXP cpp_str_to_charsexp(string s) {
	return cstr_to_charsexp(s.c_str());
}

SEXP duckdb::StringsToSexp(vector<string> s) {
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

	SEXP strings = r.Protect(Rf_allocVector(STRSXP, 5));
	SET_STRING_ELT(strings, 0, secs = Rf_mkChar("secs"));
	SET_STRING_ELT(strings, 1, mins = Rf_mkChar("mins"));
	SET_STRING_ELT(strings, 2, hours = Rf_mkChar("hours"));
	SET_STRING_ELT(strings, 3, days = Rf_mkChar("days"));
	SET_STRING_ELT(strings, 4, weeks = Rf_mkChar("weeks"));
	R_PreserveObject(strings);
	MARK_NOT_MUTABLE(strings);

	SEXP chars = r.Protect(Rf_allocVector(VECSXP, 8));
	SET_VECTOR_ELT(chars, 0, UTC_str = Rf_mkString("UTC"));
	SET_VECTOR_ELT(chars, 1, Date_str = Rf_mkString("Date"));
	SET_VECTOR_ELT(chars, 2, difftime_str = Rf_mkString("difftime"));
	SET_VECTOR_ELT(chars, 3, secs_str = Rf_mkString("secs"));
	SET_VECTOR_ELT(chars, 4, arrow_str = Rf_mkString("arrow"));
	SET_VECTOR_ELT(chars, 5, POSIXct_POSIXt_str = StringsToSexp({"POSIXct", "POSIXt"}));
	SET_VECTOR_ELT(chars, 6, factor_str = Rf_mkString("factor"));
	SET_VECTOR_ELT(chars, 7, dataframe_str = Rf_mkString("data.frame"));

	R_PreserveObject(chars);
	MARK_NOT_MUTABLE(chars);

	// Symbols don't need to be protected
	enc2utf8_sym = Rf_install("enc2utf8");
	tzone_sym = Rf_install("tzone");
	units_sym = Rf_install("units");
	getNamespace_sym = Rf_install("getNamespace");
	ImportSchema_sym = Rf_install("ImportSchema");
	ImportRecordBatch_sym = Rf_install("ImportRecordBatch");
	ImportRecordBatchReader_sym = Rf_install("ImportRecordBatchReader");
	Table__from_record_batches_sym = Rf_install("Table__from_record_batches");
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
	auto rtype = RApiTypes::DetectRType(valsexp);
	switch (rtype) {
	case RType::LOGICAL: {
		auto lgl_val = INTEGER_POINTER(valsexp)[idx];
		return RBooleanType::IsNull(lgl_val) ? Value(LogicalType::BOOLEAN) : Value::BOOLEAN(lgl_val);
	}
	case RType::INTEGER: {
		auto int_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(int_val) ? Value(LogicalType::INTEGER) : Value::INTEGER(int_val);
	}
	case RType::NUMERIC: {
		auto dbl_val = NUMERIC_POINTER(valsexp)[idx];
		bool is_null = RDoubleType::IsNull(dbl_val);
		if (is_null) {
			return Value(LogicalType::DOUBLE);
		} else {
			return Value::DOUBLE(dbl_val);
		}
	}
	case RType::STRING: {
		auto str_val = STRING_ELT(ToUtf8(valsexp), idx);
		return str_val == NA_STRING ? Value(LogicalType::VARCHAR) : Value(CHAR(str_val));
		//  TODO this does not deal with NULLs yet
		// return Value::ENUM((uint64_t)DATAPTR(str_val), LogicalType::DEDUP_POINTER_ENUM());
	}
	case RType::FACTOR: {
		auto int_val = INTEGER_POINTER(valsexp)[idx];
		auto levels = GET_LEVELS(valsexp);
		bool is_null = RIntegerType::IsNull(int_val);
		if (!is_null) {
			auto str_val = STRING_ELT(levels, int_val - 1);
			return Value(CHAR(str_val));
		} else {
			return Value(LogicalType::VARCHAR);
		}
	}
	case RType::TIMESTAMP: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		bool is_null = RTimestampType::IsNull(ts_val);
		if (!is_null) {
			return Value::TIMESTAMP(RTimestampType::Convert(ts_val));
		} else {
			return Value(LogicalType::TIMESTAMP);
		}
	}
	case RType::DATE: {
		auto d_val = NUMERIC_POINTER(valsexp)[idx];
		return RDateType::IsNull(d_val) ? Value(LogicalType::DATE) : Value::DATE(RDateType::Convert(d_val));
	}
	case RType::DATE_INTEGER: {
		auto d_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(d_val) ? Value(LogicalType::DATE) : Value::DATE(RDateType::Convert(d_val));
	}
	case RType::TIME_SECONDS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		return RTimeSecondsType::IsNull(ts_val) ? Value(LogicalType::TIME)
		                                        : Value::TIME(RTimeSecondsType::Convert(ts_val));
	}
	case RType::TIME_MINUTES: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		return RTimeMinutesType::IsNull(ts_val) ? Value(LogicalType::TIME)
		                                        : Value::TIME(RTimeMinutesType::Convert(ts_val));
	}
	case RType::TIME_HOURS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		return RTimeHoursType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeHoursType::Convert(ts_val));
	}
	case RType::TIME_DAYS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		return RTimeDaysType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeDaysType::Convert(ts_val));
	}
	case RType::TIME_WEEKS: {
		auto ts_val = NUMERIC_POINTER(valsexp)[idx];
		return RTimeWeeksType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeWeeksType::Convert(ts_val));
	}
	case RType::TIME_SECONDS_INTEGER: {
		auto ts_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeSecondsType::Convert(ts_val));
	}
	case RType::TIME_MINUTES_INTEGER: {
		auto ts_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeMinutesType::Convert(ts_val));
	}
	case RType::TIME_HOURS_INTEGER: {
		auto ts_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeHoursType::Convert(ts_val));
	}
	case RType::TIME_DAYS_INTEGER: {
		auto ts_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeDaysType::Convert(ts_val));
	}
	case RType::TIME_WEEKS_INTEGER: {
		auto ts_val = INTEGER_POINTER(valsexp)[idx];
		return RIntegerType::IsNull(ts_val) ? Value(LogicalType::TIME) : Value::TIME(RTimeWeeksType::Convert(ts_val));
	}
	default:
		cpp11::stop("duckdb_sexp_to_value: Unsupported type");
		return Value();
	}
}

SEXP RApiTypes::ValueToSexp(Value &val, string &timezone_config) {
	if (val.IsNull()) {
		return R_NilValue;
	}

	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return cpp11::logicals({val.GetValue<bool>()});
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return cpp11::integers({val.GetValue<int32_t>()});
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return cpp11::doubles({val.GetValue<double>()});
	case LogicalTypeId::VARCHAR:
		return StringsToSexp({val.ToString()});
	case LogicalTypeId::TIMESTAMP: {
		cpp11::doubles res({(double)Timestamp::GetEpochSeconds(val.GetValue<timestamp_t>())});
		// TODO bit of duplication here with statement.cpp, fix this
		// some dresssup for R
		SET_CLASS(res, RStrings::get().POSIXct_POSIXt_str);
		Rf_setAttrib(res, RStrings::get().tzone_sym, StringsToSexp({""}));
		return res;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		cpp11::doubles res({(double)Timestamp::GetEpochSeconds(val.GetValue<timestamp_t>())});
		SET_CLASS(res, RStrings::get().POSIXct_POSIXt_str);
		Rf_setAttrib(res, RStrings::get().tzone_sym, StringsToSexp({timezone_config}));
		return res;
	}
	case LogicalTypeId::TIME: {
		cpp11::doubles res({(double)val.GetValue<dtime_t>().micros / Interval::MICROS_PER_SEC});
		// some dresssup for R
		SET_CLASS(res, RStrings::get().difftime_str);
		// we always return difftime as "seconds"
		Rf_setAttrib(res, RStrings::get().units_sym, RStrings::get().secs_str);
		return res;
	}

	case LogicalTypeId::DATE: {
		cpp11::doubles res({(double)int32_t(val.GetValue<date_t>())});
		// some dresssup for R
		SET_CLASS(res, RStrings::get().Date_str);
		return res;
	}

	default:
		throw NotImplementedException("Can't convert %s of type %s", val.ToString(), val.type().ToString());
	}
}
