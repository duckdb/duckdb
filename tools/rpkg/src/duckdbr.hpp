#pragma once

#define R_NO_REMAP
#include <Rdefines.h>
#include <R_ext/Altrep.h>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

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

struct RApi {

	static SEXP Startup(SEXP dbdirsexp, SEXP readonlysexp);

	static SEXP Shutdown(SEXP dbsexp);

	static SEXP Connect(SEXP dbsexp);

	static SEXP Disconnect(SEXP connsexp);

	static SEXP Prepare(SEXP connsexp, SEXP querysexp);

	static SEXP Bind(SEXP stmtsexp, SEXP paramsexp);

	static SEXP Execute(SEXP stmtsexp);

	static SEXP Release(SEXP stmtsexp);

	static SEXP RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp);

	static SEXP UnregisterDataFrame(SEXP connsexp, SEXP namesexp);

	static SEXP RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp);

	static SEXP UnregisterArrow(SEXP connsexp, SEXP namesexp);

	static SEXP PointerToString(SEXP extptr);
	static SEXP StringsToSexp(vector<string> s);

	static RType DetectRType(SEXP v);
};

struct AltrepString {

	static R_xlen_t Length(SEXP x);

	static Rboolean Inspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int));

	static void *Dataptr(SEXP x, Rboolean writeable);
	static const void *DataptrOrNull(SEXP x);
	static SEXP Elt(SEXP x, R_xlen_t i);
	static void SetElt(SEXP x, R_xlen_t i, SEXP val);
	static int IsSorted(SEXP x);
	static int NoNA(SEXP x);
	static void Finalize(SEXP x);

	static R_altrep_class_t rclass;
};

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

struct DataFrameScanFunction : public TableFunction {
	DataFrameScanFunction();
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
        RStrings();
    };

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"


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

} // namespace duckdb