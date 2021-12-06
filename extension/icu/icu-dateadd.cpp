#include "include/icu-dateadd.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

struct ICUCalendarAdd {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarAdd");
	}
};

struct ICUCalendarSub {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarSub");
	}
};

template <>
timestamp_t ICUCalendarAdd::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar) {
	int64_t millis = timestamp.value / Interval::MICROS_PER_MSEC;
	int64_t micros = timestamp.value % Interval::MICROS_PER_MSEC;

	// Manually move the µs
	micros += interval.micros % Interval::MICROS_PER_MSEC;
	if (micros >= Interval::MICROS_PER_MSEC) {
		micros -= Interval::MICROS_PER_MSEC;
		++millis;
	} else if (micros < 0) {
		micros += Interval::MICROS_PER_MSEC;
		--millis;
	}

	// Now use the calendar to add the other parts
	UErrorCode status = U_ZERO_ERROR;
	const auto udate = UDate(millis);
	calendar->setTime(udate, status);

	// Add interval fields from lowest to highest
	calendar->add(UCAL_MILLISECOND, interval.micros / Interval::MICROS_PER_MSEC, status);
	calendar->add(UCAL_DATE, interval.days, status);
	calendar->add(UCAL_MONTH, interval.months, status);

	// Extract the new time
	millis = int64_t(calendar->getTime(status));
	if (U_FAILURE(status)) {
		throw Exception("Unable to compute ICU DATEADD.");
	}

	millis *= Interval::MICROS_PER_MSEC;
	millis += micros;
	return timestamp_t(millis);
}

template <>
timestamp_t ICUCalendarAdd::Operation(interval_t interval, timestamp_t timestamp, icu::Calendar *calendar) {
	return Operation<timestamp_t, interval_t, timestamp_t>(timestamp, interval, calendar);
}

template <>
timestamp_t ICUCalendarSub::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar) {
	const interval_t negated {-interval.months, -interval.days, -interval.micros};
	return ICUCalendarAdd::template Operation<timestamp_t, interval_t, timestamp_t>(timestamp, negated, calendar);
}

struct ICUDateAdd : public ICUDateFunc {

	template <typename TA, typename TB, typename OP>
	static ScalarFunction GetDateAddFunction(const LogicalTypeId &left_type, const LogicalTypeId &right_type) {
		return GetBinaryDateFunction<TA, TB, timestamp_t, OP>(left_type, right_type, LogicalType::TIMESTAMP_TZ);
	}

	static void AddDateAddOperators(const string &name, ClientContext &context) {
		//	temporal + interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateAddFunction<timestamp_t, interval_t, ICUCalendarAdd>(LogicalType::TIMESTAMP_TZ,
		                                                                            LogicalType::INTERVAL));
		set.AddFunction(GetDateAddFunction<interval_t, timestamp_t, ICUCalendarAdd>(LogicalType::INTERVAL,
		                                                                            LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}

	static void AddDateSubOperators(const string &name, ClientContext &context) {
		//	temporal - interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateAddFunction<timestamp_t, interval_t, ICUCalendarSub>(LogicalType::TIMESTAMP_TZ,
		                                                                            LogicalType::INTERVAL));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDateAddFunctions(ClientContext &context) {
	ICUDateAdd::AddDateAddOperators("+", context);
	ICUDateAdd::AddDateSubOperators("-", context);
}

} // namespace duckdb
