#include "include/icu-dateadd.hpp"
#include "include/icu-collate.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/timestamp.hpp"

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

	// Make sure the value is still in range
	date_t d;
	dtime_t t;
	auto us = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, Interval::MICROS_PER_MSEC);
	Timestamp::Convert(timestamp_t(us), d, t);

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

	// UDate is a double, so it can't overflow (it just loses accuracy), but converting back to µs can.
	millis = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, Interval::MICROS_PER_MSEC);
	millis = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, micros);

	// Now make sure the value is in range
	Timestamp::Convert(timestamp_t(millis), d, t);

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

struct ICUDateAdd {
	using CalendarPtr = unique_ptr<icu::Calendar>;

	struct BindData : public FunctionData {
		explicit BindData(CalendarPtr calendar_p) : calendar(move(calendar_p)) {
		}

		CalendarPtr calendar;

		unique_ptr<FunctionData> Copy() override {
			return make_unique<BindData>(CalendarPtr(calendar->clone()));
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		Value tz_value;
		string tz_id;
		if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
			tz_id = tz_value.ToString();
		}
		auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

		UErrorCode success = U_ZERO_ERROR;
		CalendarPtr calendar(icu::Calendar::createInstance(tz, success));
		if (U_FAILURE(success)) {
			throw Exception("Unable to create ICU DATEADD calendar.");
		}

		return make_unique<BindData>(move(calendar));
	}

	template <typename TA, typename TB, typename TR, typename OP>
	static void ExecuteBinary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<TA, TB, TR>(args.data[0], args.data[1], result, args.size(), [&](TA left, TB right) {
			return OP::template Operation<TA, TB, TR>(left, right, calendar.get());
		});
	}

	template <typename TA, typename TB, typename OP>
	static ScalarFunction GetDateIntervalOperator(const LogicalTypeId &left_type, const LogicalTypeId &right_type) {
		return ScalarFunction({left_type, right_type}, LogicalType::TIMESTAMP_TZ,
		                      ExecuteBinary<TA, TB, timestamp_t, OP>, false, Bind);
	}

	static void AddDateAddOperators(const string &name, ClientContext &context) {
		//	temporal + interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateIntervalOperator<timestamp_t, interval_t, ICUCalendarAdd>(LogicalType::TIMESTAMP_TZ,
		                                                                                 LogicalType::INTERVAL));
		set.AddFunction(GetDateIntervalOperator<interval_t, timestamp_t, ICUCalendarAdd>(LogicalType::INTERVAL,
		                                                                                 LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}

	static void AddDateSubOperators(const string &name, ClientContext &context) {
		//	temporal - interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateIntervalOperator<timestamp_t, interval_t, ICUCalendarSub>(LogicalType::TIMESTAMP_TZ,
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
