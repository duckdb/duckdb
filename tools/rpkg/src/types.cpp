#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb;

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
		SEXP units = Rf_getAttrib(v, Rf_install("units"));
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
		return RType::FACTOR;
	} else if (TYPEOF(v) == LGLSXP) {
		return RType::LOGICAL;
	} else if (TYPEOF(v) == INTSXP) {
		return RType::INTEGER;
	} else if (TYPEOF(v) == REALSXP) {
		if (integer64 && Rf_inherits(v, "integer64")) {
			return RType::INTEGER64;
		}
		return RType::NUMERIC;
	} else if (TYPEOF(v) == STRSXP) {
		return RType::STRING;
	}
	return RType::UNKNOWN;
}

string RApiTypes::DetectLogicalType(const LogicalType &stype, const char *caller) {
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
	return Timestamp::FromEpochSeconds(val);
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
