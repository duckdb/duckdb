#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/function/to_interval.hpp"

namespace duckdb {

template <>
bool TryMultiplyOperator::Operation(double left, int64_t right, int64_t &result) {
	return TryCast::Operation<double, int64_t>(left * double(right), result);
}

namespace {

struct ToMillenniaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::MONTHS_PER_MILLENIUM,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %s millennia out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToCenturiesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::MONTHS_PER_CENTURY,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %s centuries out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToDecadesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::MONTHS_PER_DECADE,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %s decades out of range", NumericHelper::ToString(input));
		}
		return result;
	}
};

struct ToYearsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::MONTHS_PER_YEAR,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %d years out of range", input);
		}
		return result;
	}
};

struct ToQuartersOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::MONTHS_PER_QUARTER,
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
		result.months = Cast::Operation<TA, int32_t>(input);
		;
		result.days = 0;
		result.micros = 0;
		return result;
	}
};

struct ToWeeksOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		const auto iinput = Cast::Operation<TA, int32_t>(input);
		interval_t result;
		result.months = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(iinput, Interval::DAYS_PER_WEEK, result.days)) {
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
		result.days = Cast::Operation<TA, int32_t>(input);
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

//! to_interval(part, value): builds an interval from a date part name (same vocabulary as date_part/date_diff) and a
//! count, dispatching to the to_<unit> operators above. Used by the dateadd/date_add(part, n, ts) compatibility macros.
struct ToIntervalOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA part, TB value) {
		DatePartSpecifier specifier;
		const auto part_name = part.GetString();
		if (!TryGetDatePartSpecifier(part_name, specifier)) {
			throw InvalidInputException("to_interval: date part \"%s\" not recognized", part_name);
		}
		switch (specifier) {
		case DatePartSpecifier::MILLENNIUM:
			return ToMillenniaOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::CENTURY:
			return ToCenturiesOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::DECADE:
			return ToDecadesOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::YEAR:
			return ToYearsOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::QUARTER:
			return ToQuartersOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::MONTH:
			return ToMonthsOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::WEEK:
			return ToWeeksOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::DAY:
			return ToDaysOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::HOUR:
			return ToHoursOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::MINUTE:
			return ToMinutesOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::SECOND:
			return ToSecondsOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::MILLISECONDS:
			return ToMilliSecondsOperator::Operation<TB, TR>(value);
		case DatePartSpecifier::MICROSECONDS:
			return ToMicroSecondsOperator::Operation<TB, TR>(value);
		default:
			throw InvalidInputException("to_interval: date part \"%s\" cannot be used to construct an interval",
			                            part_name);
		}
	}
};

template <typename OP>
ScalarFunctionSet GetIntegerIntervalFunctions() {
	ScalarFunctionSet function_set;
	ScalarFunction int32_fun({}, LogicalType::INTERVAL, ScalarFunction::UnaryFunction<int32_t, interval_t, OP>);
	int32_fun.GetSignature().AddParameter("integer", LogicalType::INTEGER);
	function_set.AddFunction(int32_fun);
	ScalarFunction int64_fun({}, LogicalType::INTERVAL, ScalarFunction::UnaryFunction<int64_t, interval_t, OP>);
	int64_fun.GetSignature().AddParameter("integer", LogicalType::BIGINT);
	function_set.AddFunction(int64_fun);
	function_set.SetFallible();
	return function_set;
}

} // namespace

ScalarFunctionSet ToMillenniaFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToMillenniaOperator>();
}

ScalarFunctionSet ToCenturiesFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToCenturiesOperator>();
}

ScalarFunctionSet ToDecadesFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToDecadesOperator>();
}

ScalarFunctionSet ToYearsFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToYearsOperator>();
}

ScalarFunctionSet ToQuartersFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToQuartersOperator>();
}

ScalarFunctionSet ToMonthsFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToMonthsOperator>();
}

ScalarFunctionSet ToWeeksFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToWeeksOperator>();
}

ScalarFunctionSet ToDaysFun::GetFunctions() {
	return GetIntegerIntervalFunctions<ToDaysOperator>();
}

ScalarFunction ToHoursFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToHoursOperator>);
	function.GetSignature().AddParameter("integer", LogicalType::BIGINT);
	function.SetFallible();
	return function;
}

ScalarFunction ToMinutesFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToMinutesOperator>);
	function.GetSignature().AddParameter("integer", LogicalType::BIGINT);
	function.SetFallible();
	return function;
}

ScalarFunction ToSecondsFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<double, interval_t, ToSecondsOperator>);
	function.GetSignature().AddParameter("double", LogicalType::DOUBLE);
	function.SetFallible();
	return function;
}

ScalarFunction ToMillisecondsFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<double, interval_t, ToMilliSecondsOperator>);
	function.GetSignature().AddParameter("double", LogicalType::DOUBLE);
	function.SetFallible();
	return function;
}

ScalarFunction ToIntervalFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::BinaryFunction<string_t, int64_t, interval_t, ToIntervalOperator>);
	function.GetSignature().AddParameter("part", LogicalType::VARCHAR).AddParameter("value", LogicalType::BIGINT);
	function.SetFallible();
	return function;
}

ScalarFunction ToMicrosecondsFun::GetFunction() {
	ScalarFunction function({}, LogicalType::INTERVAL,
	                        ScalarFunction::UnaryFunction<int64_t, interval_t, ToMicroSecondsOperator>);
	function.GetSignature().AddParameter("integer", LogicalType::BIGINT);
	function.SetFallible();
	return function;
}

} // namespace duckdb
