#pragma once

namespace duckdb {

struct DuckDBAltrepStringWrapper {
	std::unique_ptr<string_t[]> string_data;
	std::unique_ptr<bool[]> mask_data;
	StringHeap heap;
	idx_t length;
};

struct DuckDBAltrepListEntryWrapper {
	DuckDBAltrepListEntryWrapper(idx_t max_length);
	void Reset(idx_t offset_p, idx_t length_p);
	idx_t length;
	std::unique_ptr<data_t[]> data;
};

enum class RType {
	UNKNOWN,
	LOGICAL,
	INTEGER,
	NUMERIC,
	STRING,
	FACTOR,
	DATE,
	DATE_INTEGER,
	TIMESTAMP,
	TIME_SECONDS,
	TIME_MINUTES,
	TIME_HOURS,
	TIME_DAYS,
	TIME_WEEKS,
	TIME_SECONDS_INTEGER,
	TIME_MINUTES_INTEGER,
	TIME_HOURS_INTEGER,
	TIME_DAYS_INTEGER,
	TIME_WEEKS_INTEGER,
	INTEGER64,
};

struct RApiTypes {
	static RType DetectRType(SEXP v, bool integer64);
	static string DetectLogicalType(const LogicalType &stype, const char *caller);
	static Value SexpToValue(SEXP valsexp, R_len_t idx);
	static SEXP ValueToSexp(Value &val, string &timezone_config);
};

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
	double division = std::pow((uint64_t)10, (uint64_t)scale);
	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		dest_ptr[row_idx] =
		    !mask.RowIsValid(row_idx) ? NA_REAL : RIntegralType::DoubleCast<T>(src_ptr[row_idx]) / division;
	}
}

template <>
double RIntegralType::DoubleCast<>(hugeint_t val);

struct RDoubleType {
	static bool IsNull(double val);
	static double Convert(double val);
};

struct RDateType : public RDoubleType {
	static date_t Convert(double val);
};

struct RTimestampType : public RDoubleType {
	static timestamp_t Convert(double val);
};

struct RTimeSecondsType : public RDoubleType {
	static dtime_t Convert(double val);
};

struct RTimeMinutesType : public RDoubleType {
	static dtime_t Convert(double val);
};

struct RTimeHoursType : public RDoubleType {
	static dtime_t Convert(double val);
};

struct RTimeDaysType : public RDoubleType {
	static dtime_t Convert(double val);
};

struct RTimeWeeksType : public RDoubleType {
	static dtime_t Convert(double val);
};

struct RIntegerType {
	static bool IsNull(int val);
	static int Convert(int val);
};

struct RInteger64Type {
	static bool IsNull(int64_t val);
	static int64_t Convert(int64_t val);
};

struct RFactorType : public RIntegerType {
	static int Convert(int val);
};

struct RBooleanType : public RIntegerType {
	static bool Convert(int val);
};

} // namespace duckdb
