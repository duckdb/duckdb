#include "duckdb/core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/core_functions/to_interval.hpp"

namespace duckdb {

template <>
bool TryMultiplyOperator::Operation(double left, int64_t right, int64_t &result) {
	return TryCast::Operation<double, int64_t>(left * double(right), result);
}

struct ToMillenniaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<TA, int32_t, int32_t>(input, Interval::MONTHS_PER_MILLENIUM,
		                                                          result.months)) {
			throw OutOfRangeException("Interval value %s millennia out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToCenturiesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<TA, int32_t, int32_t>(input, Interval::MONTHS_PER_CENTURY, result.months)) {
			throw OutOfRangeException("Interval value %s centuries out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToDecadesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<TA, int32_t, int32_t>(input, Interval::MONTHS_PER_DECADE, result.months)) {
			throw OutOfRangeException("Interval value %s decades out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToYearsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(input, Interval::MONTHS_PER_YEAR,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %d years out of range", input);
		}
		return result;
	}
};

struct ToQuartersOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(input, Interval::MONTHS_PER_QUARTER,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %d quarters out of range", input);
		}
		result.days = 0;
		result.micros = 0;
		return result;
	}
};

struct ToMonthsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = input;
		result.days = 0;
		result.micros = 0;
		return result;
	}
};

struct ToWeeksOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(input, Interval::DAYS_PER_WEEK, result.days)) {
			throw OutOfRangeException("Interval value %d weeks out of range", input);
		}
		result.micros = 0;
		return result;
	}
};

struct ToDaysOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = input;
		result.micros = 0;
		return result;
	}
};

struct ToHoursOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<TA, int64_t, int64_t>(input, Interval::MICROS_PER_HOUR, result.micros)) {
			throw OutOfRangeException("Interval value %s hours out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToMinutesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<TA, int64_t, int64_t>(input, Interval::MICROS_PER_MINUTE, result.micros)) {
			throw OutOfRangeException("Interval value %s minutes out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToMilliSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<TA, int64_t, int64_t>(input, Interval::MICROS_PER_MSEC, result.micros)) {
			throw OutOfRangeException("Interval value %s milliseconds out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToMicroSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		result.micros = input;
		return result;
	}
};

ScalarFunction ToMillenniaFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToMillenniaOperator>);
}

ScalarFunction ToCenturiesFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToCenturiesOperator>);
}

ScalarFunction ToDecadesFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToDecadesOperator>);
}

ScalarFunction ToYearsFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToYearsOperator>);
}

ScalarFunction ToQuartersFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToQuartersOperator>);
}

ScalarFunction ToMonthsFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToMonthsOperator>);
}

ScalarFunction ToWeeksFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToWeeksOperator>);
}

ScalarFunction ToDaysFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToDaysOperator>);
}

ScalarFunction ToHoursFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToHoursOperator>);
}

ScalarFunction ToMinutesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToMinutesOperator>);
}

ScalarFunction ToSecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<double, interval_t, ToSecondsOperator>);
}

ScalarFunction ToMillisecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<double, interval_t, ToMilliSecondsOperator>);
}

ScalarFunction ToMicrosecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToMicroSecondsOperator>);
}

} // namespace duckdb
