#include "include/icu-dateadd.hpp"
#include "include/icu-collate.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDateAdd {
	using CalendarPtr = unique_ptr<icu::Calendar>;
	typedef void (*part_truncator_t)(icu::Calendar *calendar, uint64_t &micros);

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

	static void BinaryOperator(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		// Reorder the arguments
		idx_t interval_col = 0;
		for (; interval_col < args.ColumnCount(); ++interval_col) {
			if (args.data[interval_col].GetType().id() == LogicalTypeId::INTERVAL) {
				break;
			}
		}

		auto &interval_arg = args.data[interval_col];
		auto &date_arg = args.data[1 - interval_col];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<timestamp_t, interval_t, timestamp_t>(
		    date_arg, interval_arg, result, args.size(), [&](timestamp_t timestamp, interval_t interval) {
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
		    });
	}

	static ScalarFunction GetDateAddOperator(const LogicalTypeId &left_type, const LogicalTypeId &right_type) {
		return ScalarFunction({left_type, right_type}, LogicalType::TIMESTAMP_TZ, BinaryOperator, false, Bind);
	}

	static void AddBinaryTimestampOperator(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateAddOperator(LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL));
		set.AddFunction(GetDateAddOperator(LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDateAddFunctions(ClientContext &context) {
	ICUDateAdd::AddBinaryTimestampOperator("+", context);
}

} // namespace duckdb
