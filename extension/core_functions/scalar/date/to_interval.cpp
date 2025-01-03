#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/function/to_interval.hpp"

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
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToMillenniaOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToCenturiesFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToCenturiesOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToDecadesFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToDecadesOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToYearsFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToYearsOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToQuartersFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToQuartersOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToMonthsFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToMonthsOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToWeeksFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToWeeksOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToDaysFun::GetFunction() {
	ScalarFunction function({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int32_t, interval_t, ToDaysOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToHoursFun::GetFunction() {
	ScalarFunction function({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToHoursOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToMinutesFun::GetFunction() {
	ScalarFunction function({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToMinutesOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToSecondsFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<double, interval_t, ToSecondsOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToMillisecondsFun::GetFunction() {
	ScalarFunction function({LogicalType::DOUBLE}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<double, interval_t, ToMilliSecondsOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunction ToMicrosecondsFun::GetFunction() {
	ScalarFunction function({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToMicroSecondsOperator>);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

} // namespace duckdb
