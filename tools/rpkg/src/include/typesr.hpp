#pragma once

namespace duckdb {

struct DuckDBAltrepStringWrapper {
	vector<Vector> vectors;
	idx_t length;
};

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

struct RApiTypes {
	static RType DetectRType(SEXP v);
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
	double division = pow(10, scale);
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

struct RBooleanType : public RIntegerType {
	static bool Convert(int val);
};

} // namespace duckdb