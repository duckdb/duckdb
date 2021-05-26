#include "duckdbr.hpp"

using namespace duckdb;

RType RApi::DetectRType(SEXP v) {
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
