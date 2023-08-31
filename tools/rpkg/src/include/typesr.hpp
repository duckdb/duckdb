#pragma once

namespace duckdb {

struct DuckDBAltrepStringWrapper {
	duckdb::unsafe_unique_array<string_t> string_data;
	duckdb::unsafe_unique_array<bool> mask_data;
	StringHeap heap;
	idx_t length;
};

struct DuckDBAltrepListEntryWrapper {
	DuckDBAltrepListEntryWrapper(idx_t max_length);
	void Reset(idx_t offset_p, idx_t length_p);
	idx_t length;
	duckdb::unsafe_unique_array<data_t> data;
};

enum class RTypeId {
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
	LIST_OF_NULLS,
	BLOB,

	// No RType equivalent
	BYTE,
	LIST,
	STRUCT,
};

struct RType {
	RType();
	RType(RTypeId id); // NOLINT: Allow implicit conversion from `RTypeId`
	RType(const RType &other);
	RType(RType &&other) noexcept;

	RTypeId id() const;

	// copy assignment
	inline RType &operator=(const RType &other) {
		id_ = other.id_;
		aux_ = other.aux_;
		return *this;
	}
	// move assignment
	inline RType &operator=(RType &&other) noexcept {
		id_ = other.id_;
		std::swap(aux_, other.aux_);
		return *this;
	}

	bool operator==(const RType &rhs) const;
	inline bool operator!=(const RType &rhs) const {
		return !(*this == rhs);
	}

	static constexpr const RTypeId UNKNOWN = RTypeId::UNKNOWN;
	static constexpr const RTypeId LOGICAL = RTypeId::LOGICAL;
	static constexpr const RTypeId INTEGER = RTypeId::INTEGER;
	static constexpr const RTypeId NUMERIC = RTypeId::NUMERIC;
	static constexpr const RTypeId STRING = RTypeId::STRING;
	static constexpr const RTypeId DATE = RTypeId::DATE;
	static constexpr const RTypeId DATE_INTEGER = RTypeId::DATE_INTEGER;
	static constexpr const RTypeId TIMESTAMP = RTypeId::TIMESTAMP;
	static constexpr const RTypeId TIME_SECONDS = RTypeId::TIME_SECONDS;
	static constexpr const RTypeId TIME_MINUTES = RTypeId::TIME_MINUTES;
	static constexpr const RTypeId TIME_HOURS = RTypeId::TIME_HOURS;
	static constexpr const RTypeId TIME_DAYS = RTypeId::TIME_DAYS;
	static constexpr const RTypeId TIME_WEEKS = RTypeId::TIME_WEEKS;
	static constexpr const RTypeId TIME_SECONDS_INTEGER = RTypeId::TIME_SECONDS_INTEGER;
	static constexpr const RTypeId TIME_MINUTES_INTEGER = RTypeId::TIME_MINUTES_INTEGER;
	static constexpr const RTypeId TIME_HOURS_INTEGER = RTypeId::TIME_HOURS_INTEGER;
	static constexpr const RTypeId TIME_DAYS_INTEGER = RTypeId::TIME_DAYS_INTEGER;
	static constexpr const RTypeId TIME_WEEKS_INTEGER = RTypeId::TIME_WEEKS_INTEGER;
	static constexpr const RTypeId INTEGER64 = RTypeId::INTEGER64;
	static constexpr const RTypeId LIST_OF_NULLS = RTypeId::LIST_OF_NULLS;
	static constexpr const RTypeId BLOB = RTypeId::BLOB;

	static RType FACTOR(cpp11::strings levels);
	Vector GetFactorLevels() const;
	size_t GetFactorLevelsCount() const;
	Value GetFactorValue(int r_value) const;

	static RType LIST(const RType &child);
	RType GetListChildType() const;

	static RType STRUCT(child_list_t<RType> &&children);
	child_list_t<RType> GetStructChildTypes() const;

private:
	RTypeId id_;
	child_list_t<RType> aux_;
};

struct RApiTypes {
	static RType DetectRType(SEXP v, bool integer64);
	static LogicalType LogicalTypeFromRType(const RType &rtype, bool experimental);
	static string DetectLogicalType(const LogicalType &stype, const char *caller);
	static R_len_t GetVecSize(RType rtype, SEXP coldata);
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

struct RStringSexpType {
	static string_t Convert(SEXP val);
	static bool IsNull(SEXP val);
};

struct RSexpType {
	static bool IsNull(SEXP val);
};

struct RRawSexpType : public RSexpType {
	static string_t Convert(SEXP val);
};

} // namespace duckdb
