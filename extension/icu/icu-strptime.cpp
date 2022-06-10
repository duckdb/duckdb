#include "include/icu-strptime.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUStrptime : public ICUDateFunc {
	struct ICUStrptimeBindData : public BindData {
		ICUStrptimeBindData(ClientContext &context, const StrpTimeFormat &format) : BindData(context), format(format) {
		}
		ICUStrptimeBindData(const ICUStrptimeBindData &other) : BindData(other), format(other.format) {
		}

		StrpTimeFormat format;

		bool Equals(const FunctionData &other_p) const override {
			auto &other = (ICUStrptimeBindData &)other_p;
			return format.format_specifier == other.format.format_specifier;
		}
		unique_ptr<FunctionData> Copy() const override {
			return make_unique<ICUStrptimeBindData>(*this);
		}
	};

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
		calendar->set(UCAL_EXTENDED_YEAR, parsed.data[0]); // strptime doesn't understand eras
		calendar->set(UCAL_MONTH, parsed.data[1] - 1);
		calendar->set(UCAL_DATE, parsed.data[2]);
		calendar->set(UCAL_HOUR_OF_DAY, parsed.data[3]);
		calendar->set(UCAL_MINUTE, parsed.data[4]);
		calendar->set(UCAL_SECOND, parsed.data[5]);
		calendar->set(UCAL_MILLISECOND, parsed.data[6] / Interval::MICROS_PER_MSEC);

		// This overrides the TZ setting, so only use it if an offset was parsed.
		// Note that we don't bother/worry about the DST setting because the two just combine.
		if (format.HasFormatSpecifier(StrTimeSpecifier::UTC_OFFSET)) {
			calendar->set(UCAL_ZONE_OFFSET, parsed.data[7] * Interval::MSECS_PER_SEC * Interval::SECS_PER_MINUTE);
		}

		return GetTime(calendar, micros);
	}

	static void ICUStrptimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &str_arg = args.data[0];
		auto &fmt_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (ICUStrptimeBindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());
		auto &format = info.format;

		D_ASSERT(fmt_arg.GetVectorType() == VectorType::CONSTANT_VECTOR);

		if (ConstantVector::IsNull(fmt_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			UnaryExecutor::Execute<string_t, timestamp_t>(
			    str_arg, result, args.size(), [&](string_t input) { return Operation(calendar.get(), input, format); });
		}
	}

	static bind_scalar_function_t bind;

	static unique_ptr<FunctionData> StrpTimeBindFunction(ClientContext &context, ScalarFunction &bound_function,
	                                                     vector<unique_ptr<Expression>> &arguments) {
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("strptime format must be a constant");
		}
		Value options_str = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		StrpTimeFormat format;
		if (!options_str.IsNull()) {
			auto format_string = options_str.ToString();
			format.format_specifier = format_string;
			string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
			}

			// If we have a time zone, we should use ICU for parsing and return a TSTZ instead.
			if (format.HasFormatSpecifier(StrTimeSpecifier::TZ_NAME)) {
				bound_function.function = ICUStrptimeFunction;
				bound_function.return_type = LogicalType::TIMESTAMP_TZ;
				return make_unique<ICUStrptimeBindData>(context, format);
			}
		}

		// Fall back to faster, non-TZ parsing
		bound_function.bind = bind;
		return bind(context, bound_function, arguments);
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		// Find the old function
		auto &catalog = Catalog::GetCatalog(context);
		auto entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, name);
		D_ASSERT(entry && entry->type == CatalogType::SCALAR_FUNCTION_ENTRY);
		auto &func = (ScalarFunctionCatalogEntry &)*entry;
		vector<LogicalType> types {LogicalType::VARCHAR, LogicalType::VARCHAR};
		string error;
		bool cast_parameters;
		const idx_t best_function = Function::BindFunction(func.name, func.functions, types, error, cast_parameters);
		if (best_function == DConstants::INVALID_INDEX) {
			return;
		}

		// Tail patch the old binder
		auto &bound_function = func.functions[best_function];
		bind = bound_function.bind;
		bound_function.bind = StrpTimeBindFunction;
	}
};

bind_scalar_function_t ICUStrptime::bind = nullptr;

struct ICUStrftime : public ICUDateFunc {
	static void ParseFormatSpecifier(string_t &format_str, StrfTimeFormat &format) {
		const auto format_specifier = format_str.GetString();
		const auto error = StrTimeFormat::ParseFormatSpecifier(format_specifier, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format_specifier, error);
		}
	}

	static string_t Operation(icu::Calendar *calendar, timestamp_t input, const char *tz_name, StrfTimeFormat &format,
	                          Vector &result) {
		// Now get the parts in the given time zone
		uint64_t micros = SetTime(calendar, input);

		int32_t data[8];
		data[0] = ExtractField(calendar, UCAL_EXTENDED_YEAR); // strftime doesn't understand eras.
		data[1] = ExtractField(calendar, UCAL_MONTH) + 1;
		data[2] = ExtractField(calendar, UCAL_DATE);
		data[3] = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		data[4] = ExtractField(calendar, UCAL_MINUTE);
		data[5] = ExtractField(calendar, UCAL_SECOND);
		data[6] = ExtractField(calendar, UCAL_MILLISECOND) * Interval::MICROS_PER_MSEC + micros;

		data[7] = ExtractField(calendar, UCAL_ZONE_OFFSET) + ExtractField(calendar, UCAL_DST_OFFSET);
		data[7] /= Interval::MSECS_PER_SEC;
		data[7] /= Interval::SECS_PER_MINUTE;

		const auto date = Date::FromDate(data[0], data[1], data[2]);
		const auto time = Time::FromTime(data[3], data[4], data[5], data[6]);

		const auto len = format.GetLength(date, time, data[7], tz_name);
		string_t target = StringVector::EmptyString(result, len);
		format.FormatString(date, data, tz_name, target.GetDataWriteable());
		target.Finalize();

		return target;
	}

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

				UnaryExecutor::ExecuteWithNulls<timestamp_t, string_t>(
				    src_arg, result, args.size(), [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
					    if (Timestamp::IsFinite(input)) {
						    return Operation(calendar.get(), input, tz_name, format, result);
					    } else {
						    mask.SetInvalid(idx);
						    return string_t();
					    }
				    });
			}
		} else {
			BinaryExecutor::ExecuteWithNulls<timestamp_t, string_t, string_t>(
			    src_arg, fmt_arg, result, args.size(),
			    [&](timestamp_t input, string_t format_specifier, ValidityMask &mask, idx_t idx) {
				    if (Timestamp::IsFinite(input)) {
					    StrfTimeFormat format;
					    ParseFormatSpecifier(format_specifier, format);

					    return Operation(calendar.get(), input, tz_name, format, result);
				    } else {
					    mask.SetInvalid(idx);
					    return string_t();
				    }
			    });
		}
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                               ICUStrftimeFunction, false, false, Bind));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUStrptimeFunctions(ClientContext &context) {
	ICUStrptime::AddBinaryTimestampFunction("strptime", context);
	ICUStrftime::AddBinaryTimestampFunction("strftime", context);
}

} // namespace duckdb
