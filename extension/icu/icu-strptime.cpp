#include "include/icu-strptime.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUStrptime : public ICUDateFunc {
	static void ParseFormatSpecifier(string_t &format_specifier, StrpTimeFormat &format) {
		format.format_specifier = format_specifier.GetString();
		const auto error = StrTimeFormat::ParseFormatSpecifier(format.format_specifier, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format.format_specifier, error);
		}
	}

	static timestamp_t Operation(icu::Calendar *calendar, string_t input, StrpTimeFormat &format) {
		StrpTimeFormat::ParseResult parsed;
		format.Parse(input, parsed);
		if (!parsed.error_message.empty()) {
			throw InvalidInputException(StrpTimeFormat::FormatError(parsed, input, format.format_specifier));
		}

		// Set TZ first, if any.
		// Note that empty TZ names are not allowed,
		// but unknown names will map to GMT.
		if (!parsed.tz.empty()) {
			SetTimeZone(calendar, parsed.tz);
		}

		// Now get the parts in the given time zone
		uint64_t micros = 0;
		calendar->set(UCAL_YEAR, parsed.data[0]);
		calendar->set(UCAL_MONTH, parsed.data[1] - 1);
		calendar->set(UCAL_DATE, parsed.data[2]);
		calendar->set(UCAL_HOUR_OF_DAY, parsed.data[3]);
		calendar->set(UCAL_MINUTE, parsed.data[4]);
		calendar->set(UCAL_SECOND, parsed.data[5]);
		calendar->set(UCAL_MILLISECOND, parsed.data[6] / Interval::MICROS_PER_MSEC);
		return GetTime(calendar, micros);
	}

	static void ICUStrptimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &str_arg = args.data[0];
		auto &fmt_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		if (fmt_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(fmt_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				StrpTimeFormat format;
				ParseFormatSpecifier(*ConstantVector::GetData<string_t>(fmt_arg), format);

				UnaryExecutor::Execute<string_t, timestamp_t>(str_arg, result, args.size(), [&](string_t input) {
					return Operation(calendar.get(), input, format);
				});
			}
		} else {
			BinaryExecutor::Execute<string_t, string_t, timestamp_t>(
			    str_arg, fmt_arg, result, args.size(), [&](string_t input, string_t format_specifier) {
				    StrpTimeFormat format;
				    ParseFormatSpecifier(format_specifier, format);
				    SetTimeZone(calendar.get(), info.tz_setting);
				    return Operation(calendar.get(), input, format);
			    });
		}
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::TIMESTAMP_TZ,
		                               ICUStrptimeFunction, false, false, Bind));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

struct ICUStrftime : public ICUDateFunc {
	static void ParseFormatSpecifier(string_t &format_str, StrfTimeFormat &format) {
		const auto format_specifier = format_str.GetString();
		const auto error = StrTimeFormat::ParseFormatSpecifier(format_specifier, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format_specifier, error);
		}
	}

	static string Operation(icu::Calendar *calendar, timestamp_t input, const char *tz_name, StrfTimeFormat &format) {
		// Now get the parts in the given time zone
		uint64_t micros = SetTime(calendar, input);

		int32_t data[7];
		data[0] = ExtractField(calendar, UCAL_YEAR);
		data[1] = ExtractField(calendar, UCAL_MONTH) + 1;
		data[2] = ExtractField(calendar, UCAL_DATE);
		data[3] = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		data[4] = ExtractField(calendar, UCAL_MINUTE);
		data[5] = ExtractField(calendar, UCAL_SECOND);
		data[6] = ExtractField(calendar, UCAL_MILLISECOND) * Interval::MICROS_PER_MSEC + micros;

		date_t date;
		Date::Convert(date, data[0], data[1], data[2]);
		dtime_t time;
		Time::Convert(time, data[3], data[4], data[5], data[6]);
		auto len = format.GetLength(date, time, tz_name);
		auto result = unique_ptr<char[]>(new char[len]);

		format.FormatString(date, data, tz_name, result.get());
		return string(result.get(), len);
	}

	static string Operation(icu::Calendar *calendar, date_t input, const char *tz_name, StrfTimeFormat &format) {
		return Operation(calendar, Timestamp::FromDatetime(input, dtime_t(0)), tz_name, format);
	}

	template <typename TA>
	static void ICUStrftimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &src_arg = args.data[0];
		auto &fmt_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());
		const auto tz_name = info.tz_setting.c_str();

		if (fmt_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(fmt_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				StrfTimeFormat format;
				ParseFormatSpecifier(*ConstantVector::GetData<string_t>(fmt_arg), format);

				UnaryExecutor::Execute<TA, string_t>(src_arg, result, args.size(), [&](TA input) {
					return Operation(calendar.get(), input, tz_name, format);
				});
			}
		} else {
			BinaryExecutor::Execute<TA, string_t, string_t>(
			    src_arg, fmt_arg, result, args.size(), [&](TA input, string_t format_specifier) {
				    StrfTimeFormat format;
				    ParseFormatSpecifier(format_specifier, format);
				    SetTimeZone(calendar.get(), info.tz_setting);
				    return Operation(calendar.get(), input, tz_name, format);
			    });
		}
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                               ICUStrftimeFunction<timestamp_t>, false, false, Bind));
		set.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                               ICUStrftimeFunction<date_t>, false, false, Bind));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUStrptimeFunctions(ClientContext &context) {
	ICUStrptime::AddBinaryTimestampFunction("strptime_icu", context);
	ICUStrftime::AddBinaryTimestampFunction("strftime_icu", context);
}

} // namespace duckdb
