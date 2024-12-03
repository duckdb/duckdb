#include "include/icu-current.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct ICUCurrent : public ICUDateFunc {

	struct CurrentDate {
		static bool Operation(icu::Calendar *calendar, ExpressionState &state, date_t &result) {
			(void)SetCurrentTimestamp(calendar, state);
			const auto year = ExtractField(calendar, UCAL_YEAR);
			const auto month = ExtractField(calendar, UCAL_MONTH) + 1;
			const auto day = ExtractField(calendar, UCAL_DATE);
			// If we are in a calendar with 13 months, this will fail.
			return Date::TryFromDate(year, month, day, result);
		}
	};

	struct CurrentTime {
		static bool Operation(icu::Calendar *calendar, ExpressionState &state, dtime_tz_t &result) {
			auto micros = UnsafeNumericCast<int32_t>(SetCurrentTimestamp(calendar, state));

			//	Time in current TZ
			const auto hour = ExtractField(calendar, UCAL_HOUR_OF_DAY);
			const auto minute = ExtractField(calendar, UCAL_MINUTE);
			const auto second = ExtractField(calendar, UCAL_SECOND);
			const auto millis = ExtractField(calendar, UCAL_MILLISECOND);
			micros += millis * int32_t(Interval::MICROS_PER_MSEC);
			if (!Time::IsValidTime(hour, minute, second, micros)) {
				return false;
			}
			const auto time = Time::FromTime(hour, minute, second, micros);

			//	Offset in current TZ
			auto offset = ExtractField(calendar, UCAL_ZONE_OFFSET);
			offset += ExtractField(calendar, UCAL_DST_OFFSET);
			offset /= Interval::MSECS_PER_SEC;

			result = dtime_tz_t(time, offset);
			return true;
		}
	};

	template <typename RESULT_TYPE, typename OP>
	static void CurrentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 0);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<ICUDateFunc::BindData>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		RESULT_TYPE current;
		if (OP::Operation(calendar, state, current)) {
			auto value = Value::CreateValue(current);
			result.Reference(value);
		} else {
			// No templated NULL constructor?
			auto value = Value::CreateValue(current);
			Value null = Value(value.type());
			result.Reference(null);
		}
	}

	template <typename RESULT_TYPE, typename OP>
	static void AddNullaryFunctions(const string &name, DatabaseInstance &db, const LogicalType &result_type) {
		ScalarFunctionSet set(name);
		ScalarFunction func({}, result_type, CurrentFunction<RESULT_TYPE, OP>, ICUCurrent::Bind);
		func.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
		set.AddFunction(func);
		ExtensionUtil::RegisterFunction(db, set);
	}
};

void RegisterICUCurrentFunctions(DatabaseInstance &db) {
	ICUCurrent::AddNullaryFunctions<date_t, ICUCurrent::CurrentDate>("current_date", db, LogicalType::DATE);
	ICUCurrent::AddNullaryFunctions<date_t, ICUCurrent::CurrentDate>("today", db, LogicalType::DATE);
	ICUCurrent::AddNullaryFunctions<dtime_tz_t, ICUCurrent::CurrentTime>("get_current_time", db, LogicalType::TIME_TZ);
}

} // namespace duckdb
