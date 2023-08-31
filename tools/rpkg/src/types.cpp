#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb;

RType::RType() : id_(RTypeId::UNKNOWN) {
}

RType::RType(RTypeId id) : id_(id) {
}

RType::RType(const RType &other) : id_(other.id_), aux_(other.aux_) {
}

RType::RType(RType &&other) noexcept : id_(other.id_), aux_(std::move(other.aux_)) {
}

RTypeId RType::id() const {
	return id_;
}

bool RType::operator==(const RType &rhs) const {
	return id_ == rhs.id_ && aux_ == rhs.aux_;
}

RType RType::FACTOR(cpp11::strings levels) {
	RType out = RType(RTypeId::FACTOR);
	for (R_xlen_t level_idx = 0; level_idx < levels.size(); level_idx++) {
		out.aux_.push_back(std::make_pair(levels[level_idx], RType()));
	}
	return out;
}

Vector RType::GetFactorLevels() const {
	D_ASSERT(id_ == RTypeId::FACTOR);
	Vector duckdb_levels(LogicalType::VARCHAR, aux_.size());
	auto levels_ptr = FlatVector::GetData<string_t>(duckdb_levels);
	for (size_t level_idx = 0; level_idx < aux_.size(); level_idx++) {
		levels_ptr[level_idx] = StringVector::AddString(duckdb_levels, aux_[level_idx].first);
	}
	return duckdb_levels;
}

size_t RType::GetFactorLevelsCount() const {
	D_ASSERT(id_ == RTypeId::FACTOR);
	return aux_.size();
}

Value RType::GetFactorValue(int r_value) const {
	D_ASSERT(id_ == RTypeId::FACTOR);
	bool is_null = RIntegerType::IsNull(r_value);
	if (!is_null) {
		auto str_val = aux_[r_value - 1].first;
		return Value(str_val);
	} else {
		return Value(LogicalType::VARCHAR);
	}
}

RType RType::LIST(const RType &child) {
	RType out = RType(RTypeId::LIST);
	out.aux_.push_back(std::make_pair("", child));
	return out;
}

RType RType::GetListChildType() const {
	D_ASSERT(id_ == RTypeId::LIST);
	return aux_.front().second;
}

RType RType::STRUCT(child_list_t<RType> &&children) {
	RType out = RType(RTypeId::STRUCT);
	std::swap(out.aux_, children);
	return out;
}

child_list_t<RType> RType::GetStructChildTypes() const {
	D_ASSERT(id_ == RTypeId::STRUCT);
	return aux_;
}

RType RApiTypes::DetectRType(SEXP v, bool integer64) {
	if (TYPEOF(v) == REALSXP && Rf_inherits(v, "POSIXct")) {
		return RType::TIMESTAMP;
	} else if (TYPEOF(v) == REALSXP && Rf_inherits(v, "Date")) {
		return RType::DATE;
	} else if (TYPEOF(v) == INTSXP && Rf_inherits(v, "Date")) {
		return RType::DATE_INTEGER;
	} else if (TYPEOF(v) == REALSXP && Rf_inherits(v, "difftime")) {
		SEXP units = Rf_getAttrib(v, RStrings::get().units_sym);
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
	} else if (TYPEOF(v) == INTSXP && Rf_inherits(v, "difftime")) {
		SEXP units = Rf_getAttrib(v, RStrings::get().units_sym);
		if (TYPEOF(units) != STRSXP) {
			return RType::UNKNOWN;
		}
		SEXP units0 = STRING_ELT(units, 0);
		if (units0 == RStrings::get().secs) {
			return RType::TIME_SECONDS_INTEGER;
		} else if (units0 == RStrings::get().mins) {
			return RType::TIME_MINUTES_INTEGER;
		} else if (units0 == RStrings::get().hours) {
			return RType::TIME_HOURS_INTEGER;
		} else if (units0 == RStrings::get().days) {
			return RType::TIME_DAYS_INTEGER;
		} else if (units0 == RStrings::get().weeks) {
			return RType::TIME_WEEKS_INTEGER;
		} else {
			return RType::UNKNOWN;
		}
	} else if (Rf_isFactor(v) && TYPEOF(v) == INTSXP) {
		return RType::FACTOR(GET_LEVELS(v));
	} else if (TYPEOF(v) == LGLSXP) {
		return RType::LOGICAL;
	} else if (TYPEOF(v) == INTSXP) {
		return RType::INTEGER;
	} else if (TYPEOF(v) == RAWSXP) {
		return RTypeId::BYTE;
	} else if (TYPEOF(v) == REALSXP) {
		if (integer64 && Rf_inherits(v, "integer64")) {
			return RType::INTEGER64;
		}
		return RType::NUMERIC;
	} else if (TYPEOF(v) == STRSXP) {
		return RType::STRING;
	} else if (TYPEOF(v) == VECSXP) {
		if (Rf_inherits(v, "blob")) {
			return RType::BLOB;
		}

		if (Rf_inherits(v, "data.frame")) {
			child_list_t<RType> child_types;
			R_xlen_t ncol = Rf_length(v);
			SEXP names = GET_NAMES(v);

			for (R_xlen_t i = 0; i < ncol; ++i) {
				RType child = DetectRType(VECTOR_ELT(v, i), integer64);
				if (child == RType::UNKNOWN) {
					return (RType::UNKNOWN);
				}
				child_types.push_back(std::make_pair(CHAR(STRING_ELT(names, i)), child));
			}

			return RType::STRUCT(std::move(child_types));
		} else {
			R_xlen_t len = Rf_xlength(v);
			R_xlen_t i = 0;
			auto type = RType();
			for (; i < len; ++i) {
				auto elt = VECTOR_ELT(v, i);
				if (elt != R_NilValue) {
					type = DetectRType(elt, integer64);
					break;
				}
			}

			if (i == len) {
				return RType::LIST_OF_NULLS;
			}

			for (; i < len; ++i) {
				auto elt = VECTOR_ELT(v, i);
				if (elt != R_NilValue) {
					auto new_type = DetectRType(elt, integer64);
					if (new_type != type) {
						return RType::UNKNOWN;
					}
				}
			}

			if (type == RTypeId::BYTE) {
				return RType::BLOB;
			}

			return RType::LIST(type);
		}
	}
	return RType::UNKNOWN;
}

LogicalType RApiTypes::LogicalTypeFromRType(const RType &rtype, bool experimental) {
	switch (rtype.id()) {
	case RType::LOGICAL:
		return LogicalType::BOOLEAN;
	case RType::INTEGER:
		return LogicalType::INTEGER;
	case RType::NUMERIC:
		return LogicalType::DOUBLE;
	case RType::INTEGER64:
		return LogicalType::BIGINT;
	case RTypeId::FACTOR: {
		auto duckdb_levels = rtype.GetFactorLevels();
		return LogicalType::ENUM(duckdb_levels, rtype.GetFactorLevelsCount());
	}
	case RType::STRING:
		if (experimental) {
			return RStringsType::Get();
		} else {
			return LogicalType::VARCHAR;
		}
		break;
	case RType::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case RType::TIME_SECONDS:
	case RType::TIME_MINUTES:
	case RType::TIME_HOURS:
	case RType::TIME_DAYS:
	case RType::TIME_WEEKS:
		return LogicalType::TIME;
	case RType::TIME_SECONDS_INTEGER:
	case RType::TIME_MINUTES_INTEGER:
	case RType::TIME_HOURS_INTEGER:
	case RType::TIME_DAYS_INTEGER:
	case RType::TIME_WEEKS_INTEGER:
		return LogicalType::TIME;
	case RType::DATE:
		return LogicalType::DATE;
	case RType::DATE_INTEGER:
		return LogicalType::DATE;
	case RType::LIST_OF_NULLS:
	case RType::BLOB:
		return LogicalType::BLOB;
	case RTypeId::LIST:
		return LogicalType::LIST(RApiTypes::LogicalTypeFromRType(rtype.GetListChildType(), experimental));
	case RTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (const auto &child : rtype.GetStructChildTypes()) {
			children.push_back(
			    std::make_pair(child.first, RApiTypes::LogicalTypeFromRType(child.second, experimental)));
		}
		if (children.size() == 0) {
			cpp11::stop("rapi_execute: Packed column must have at least one column");
		}
		return LogicalType::STRUCT(std::move(children));
	}

	default:
		cpp11::stop("rapi_execute: Can't convert R type to logical type");
	}
}

string RApiTypes::DetectLogicalType(const LogicalType &stype, const char *caller) {

	if (stype.GetAlias() == R_STRING_TYPE_NAME) {
		return "character";
	}

	switch (stype.id()) {
	case LogicalTypeId::BOOLEAN:
		return "logical";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		return "integer";
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
		return "POSIXct";
	case LogicalTypeId::DATE:
		return "Date";
	case LogicalTypeId::TIME:
	case LogicalTypeId::INTERVAL:
		return "difftime";
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return "numeric";
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::UUID:
		return "character";
	case LogicalTypeId::BLOB:
		return "raw";
	case LogicalTypeId::LIST:
		return "list";
	case LogicalTypeId::STRUCT:
		return "data.frame";
	case LogicalTypeId::ENUM:
		return "factor";
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::SQLNULL:
		return "unknown";
	default:
		cpp11::stop("%s: Unknown column type for prepare: %s", caller, stype.ToString().c_str());
		break;
	}
}

bool RDoubleType::IsNull(double val) {
	return ISNA(val);
}

double RDoubleType::Convert(double val) {
	return val;
}

date_t RDateType::Convert(double val) {
	return date_t((int32_t)val);
}

timestamp_t RTimestampType::Convert(double val) {
	return Timestamp::FromEpochMicroSeconds(round(val * Interval::MICROS_PER_SEC));
}

dtime_t RTimeSecondsType::Convert(double val) {
	return dtime_t(int64_t(val * Interval::MICROS_PER_SEC));
}

dtime_t RTimeMinutesType::Convert(double val) {
	return dtime_t(int64_t(val * Interval::MICROS_PER_MINUTE));
}

dtime_t RTimeHoursType::Convert(double val) {
	return dtime_t(int64_t(val * Interval::MICROS_PER_HOUR));
}

dtime_t RTimeDaysType::Convert(double val) {
	return dtime_t(int64_t(val * Interval::MICROS_PER_DAY));
}

dtime_t RTimeWeeksType::Convert(double val) {
	return dtime_t(int64_t(val * (Interval::MICROS_PER_DAY * Interval::DAYS_PER_WEEK)));
}

bool RIntegerType::IsNull(int val) {
	return val == NA_INTEGER;
}

int RIntegerType::Convert(int val) {
	return val;
}

bool RInteger64Type::IsNull(int64_t val) {
	return val == NumericLimits<int64_t>::Minimum();
}

int64_t RInteger64Type::Convert(int64_t val) {
	return val;
}

int RFactorType::Convert(int val) {
	return val - 1;
}

bool RBooleanType::Convert(int val) {
	return val;
}

template <>
double RIntegralType::DoubleCast<>(hugeint_t val) {
	return Hugeint::Cast<double>(val);
}

string_t RStringSexpType::Convert(SEXP val) {
	return string_t((char *)CHAR(val));
}

bool RStringSexpType::IsNull(SEXP val) {
	return val == NA_STRING;
}

bool RSexpType::IsNull(SEXP val) {
	return val == R_NilValue;
}

string_t RRawSexpType::Convert(SEXP val) {
	return string_t((char *)RAW(val), Rf_xlength(val));
}
