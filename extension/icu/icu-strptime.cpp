#include "include/icu-strptime.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
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
			throw InvalidInputException(parsed.FormatError(input, format.format_specifier));
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

void RegisterICUStrptimeFunctions(ClientContext &context) {
	ICUStrptime::AddBinaryTimestampFunction("strptime_icu", context);
}

} // namespace duckdb
