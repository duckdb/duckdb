#include "include/icu-strptime.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

struct ICUStrptime : public ICUDateFunc {
	using ParseResult = StrpTimeFormat::ParseResult;

	struct ICUStrptimeBindData : public BindData {
		ICUStrptimeBindData(ClientContext &context, const StrpTimeFormat &format)
		    : BindData(context), formats(1, format) {
		}
		ICUStrptimeBindData(ClientContext &context, vector<StrpTimeFormat> formats_p)
		    : BindData(context), formats(std::move(formats_p)) {
		}
		ICUStrptimeBindData(const ICUStrptimeBindData &other) : BindData(other), formats(other.formats) {
		}

		vector<StrpTimeFormat> formats;

		bool Equals(const FunctionData &other_p) const override {
			auto &other = other_p.Cast<ICUStrptimeBindData>();
			if (formats.size() != other.formats.size()) {
				return false;
			}
			for (size_t i = 0; i < formats.size(); ++i) {
				if (formats[i].format_specifier != other.formats[i].format_specifier) {
					return false;
				}
			}
			return true;
		}
		duckdb::unique_ptr<FunctionData> Copy() const override {
			return make_uniq<ICUStrptimeBindData>(*this);
		}
	};

	static void ParseFormatSpecifier(string_t &format_specifier, StrpTimeFormat &format) {
		format.format_specifier = format_specifier.GetString();
		const auto error = StrTimeFormat::ParseFormatSpecifier(format.format_specifier, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format.format_specifier, error);
		}
	}

	static uint64_t ToMicros(icu::Calendar *calendar, const ParseResult &parsed, const StrpTimeFormat &format) {
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

		return micros;
	}

	static void Parse(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &str_arg = args.data[0];
		auto &fmt_arg = args.data[1];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<ICUStrptimeBindData>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();
		auto &formats = info.formats;

		D_ASSERT(fmt_arg.GetVectorType() == VectorType::CONSTANT_VECTOR);

		if (ConstantVector::IsNull(fmt_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			UnaryExecutor::Execute<string_t, timestamp_t>(str_arg, result, args.size(), [&](string_t input) {
				ParseResult parsed;
				for (auto &format : info.formats) {
					if (format.Parse(input, parsed)) {
						return GetTime(calendar, ToMicros(calendar, parsed, format));
					}
				}

				throw InvalidInputException(parsed.FormatError(input, info.formats[0].format_specifier));
			});
		}
	}

	static void TryParse(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &str_arg = args.data[0];
		auto &fmt_arg = args.data[1];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<ICUStrptimeBindData>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();
		auto &formats = info.formats;

		D_ASSERT(fmt_arg.GetVectorType() == VectorType::CONSTANT_VECTOR);

		if (ConstantVector::IsNull(fmt_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			UnaryExecutor::ExecuteWithNulls<string_t, timestamp_t>(
			    str_arg, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    ParseResult parsed;
				    for (auto &format : info.formats) {
					    if (format.Parse(input, parsed)) {
						    timestamp_t result;
						    if (TryGetTime(calendar, ToMicros(calendar, parsed, format), result)) {
							    return result;
						    }
					    }
				    }

				    mask.SetInvalid(idx);
				    return timestamp_t();
			    });
		}
	}

	static bind_scalar_function_t bind_strptime;

	static duckdb::unique_ptr<FunctionData> StrpTimeBindFunction(ClientContext &context, ScalarFunction &bound_function,
	                                                             vector<duckdb::unique_ptr<Expression>> &arguments) {
		if (arguments[1]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("strptime format must be a constant");
		}
		scalar_function_t function = (bound_function.name == "try_strptime") ? TryParse : Parse;
		Value format_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		string format_string;
		StrpTimeFormat format;
		if (format_value.IsNull()) {
			;
		} else if (format_value.type().id() == LogicalTypeId::VARCHAR) {
			format_string = format_value.ToString();
			format.format_specifier = format_string;
			string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
			}

			// If we have a time zone, we should use ICU for parsing and return a TSTZ instead.
			if (format.HasFormatSpecifier(StrTimeSpecifier::TZ_NAME)) {
				bound_function.function = function;
				bound_function.return_type = LogicalType::TIMESTAMP_TZ;
				return make_uniq<ICUStrptimeBindData>(context, format);
			}
		} else if (format_value.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
			const auto &children = ListValue::GetChildren(format_value);
			if (children.empty()) {
				throw InvalidInputException("strptime format list must not be empty");
			}
			vector<StrpTimeFormat> formats;
			bool has_tz = true;
			for (const auto &child : children) {
				format_string = child.ToString();
				format.format_specifier = format_string;
				string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
				if (!error.empty()) {
					throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
				}
				// If any format has UTC offsets, then we have to produce TSTZ
				has_tz = has_tz || format.HasFormatSpecifier(StrTimeSpecifier::TZ_NAME);
				formats.emplace_back(format);
			}
			if (has_tz) {
				bound_function.function = function;
				bound_function.return_type = LogicalType::TIMESTAMP_TZ;
				return make_uniq<ICUStrptimeBindData>(context, formats);
			}
		}

		// Fall back to faster, non-TZ parsing
		bound_function.bind = bind_strptime;
		return bind_strptime(context, bound_function, arguments);
	}

	static void TailPatch(const string &name, ClientContext &context, const vector<LogicalType> &types) {
		// Find the old function
		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, name);
		D_ASSERT(entry.type == CatalogType::SCALAR_FUNCTION_ENTRY);
		auto &func = entry.Cast<ScalarFunctionCatalogEntry>();
		string error;

		FunctionBinder function_binder(context);
		const idx_t best_function = function_binder.BindFunction(func.name, func.functions, types, error);
		if (best_function == DConstants::INVALID_INDEX) {
			return;
		}

		// Tail patch the old binder
		auto &bound_function = func.functions.GetFunctionReferenceByOffset(best_function);
		bind_strptime = bound_function.bind;
		bound_function.bind = StrpTimeBindFunction;
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		vector<LogicalType> types {LogicalType::VARCHAR, LogicalType::VARCHAR};
		TailPatch(name, context, types);

		types[1] = LogicalType::LIST(LogicalType::VARCHAR);
		TailPatch(name, context, types);
	}

	static bool CastFromVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr cal(info.calendar->clone());

		UnaryExecutor::ExecuteWithNulls<string_t, timestamp_t>(
		    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
			    timestamp_t result;
			    const auto str = input.GetData();
			    const auto len = input.GetSize();
			    string_t tz(nullptr, 0);
			    bool has_offset = false;
			    if (!Timestamp::TryConvertTimestampTZ(str, len, result, has_offset, tz)) {
				    auto msg = Timestamp::ConversionError(string(str, len));
				    HandleCastError::AssignError(msg, parameters.error_message);
				    mask.SetInvalid(idx);
			    } else if (!has_offset) {
				    // Convert parts to a TZ (default or parsed) if no offset was provided
				    auto calendar = cal.get();

				    // Change TZ if one was provided.
				    if (tz.GetSize()) {
					    SetTimeZone(calendar, tz);
				    }

				    // Now get the parts in the given time zone
				    result = FromNaive(calendar, result);
			    }

			    return result;
		    });
		return true;
	}

	static BoundCastInfo BindCastFromVarchar(BindCastInput &input, const LogicalType &source,
	                                         const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for VARCHAR to TIMESTAMPTZ cast.");
		}

		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

		return BoundCastInfo(CastFromVarchar, std::move(cast_data));
	}

	static void AddCasts(ClientContext &context) {
		auto &config = DBConfig::GetConfig(context);
		auto &casts = config.GetCastFunctions();

		casts.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ, BindCastFromVarchar);
	}
};

bind_scalar_function_t ICUStrptime::bind_strptime = nullptr;

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
		// Infinity is always formatted the same way
		if (!Timestamp::IsFinite(input)) {
			return StringVector::AddString(result, Timestamp::ToString(input));
		}

		// Get the parts in the given time zone
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

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
		                               ICUStrftimeFunction, Bind));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetSystemCatalog(context);
		catalog.AddFunction(context, func_info);
	}

	static string_t CastOperation(icu::Calendar *calendar, timestamp_t input, Vector &result) {
		// Infinity is always formatted the same way
		if (!Timestamp::IsFinite(input)) {
			return StringVector::AddString(result, Timestamp::ToString(input));
		}

		// Get the parts in the given time zone
		uint64_t micros = SetTime(calendar, input);

		int32_t date_units[3];
		date_units[0] = ExtractField(calendar, UCAL_EXTENDED_YEAR); // strftime doesn't understand eras.
		date_units[1] = ExtractField(calendar, UCAL_MONTH) + 1;
		date_units[2] = ExtractField(calendar, UCAL_DATE);

		int32_t time_units[4];
		time_units[0] = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		time_units[1] = ExtractField(calendar, UCAL_MINUTE);
		time_units[2] = ExtractField(calendar, UCAL_SECOND);
		time_units[3] = ExtractField(calendar, UCAL_MILLISECOND) * Interval::MICROS_PER_MSEC + micros;

		idx_t year_length;
		bool add_bc;
		const auto date_len = DateToStringCast::Length(date_units, year_length, add_bc);

		char micro_buffer[6];
		const auto time_len = TimeToStringCast::Length(time_units, micro_buffer);

		auto offset = ExtractField(calendar, UCAL_ZONE_OFFSET) + ExtractField(calendar, UCAL_DST_OFFSET);
		offset /= Interval::MSECS_PER_SEC;
		offset /= Interval::SECS_PER_MINUTE;
		int hour_offset = offset / 60;
		int minute_offset = offset % 60;
		auto offset_str = Time::ToUTCOffset(hour_offset, minute_offset);
		const auto offset_len = offset_str.size();

		const auto len = date_len + 1 + time_len + offset_len;
		string_t target = StringVector::EmptyString(result, len);
		auto buffer = target.GetDataWriteable();

		DateToStringCast::Format(buffer, date_units, year_length, add_bc);
		buffer += date_len;
		*buffer++ = ' ';

		TimeToStringCast::Format(buffer, time_len, time_units, micro_buffer);
		buffer += time_len;

		memcpy(buffer, offset_str.c_str(), offset_len);
		buffer += offset_len;

		target.Finalize();

		return target;
	}

	static bool CastToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::ExecuteWithNulls<timestamp_t, string_t>(source, result, count,
		                                                       [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
			                                                       return CastOperation(calendar.get(), input, result);
		                                                       });
		return true;
	}

	static BoundCastInfo BindCastToVarchar(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for TIMESTAMPTZ to VARCHAR cast.");
		}

		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

		return BoundCastInfo(CastToVarchar, std::move(cast_data));
	}

	static void AddCasts(ClientContext &context) {
		auto &config = DBConfig::GetConfig(context);
		auto &casts = config.GetCastFunctions();

		casts.RegisterCastFunction(LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR, BindCastToVarchar);
	}
};

void RegisterICUStrptimeFunctions(ClientContext &context) {
	ICUStrptime::AddBinaryTimestampFunction("strptime", context);
	ICUStrptime::AddBinaryTimestampFunction("try_strptime", context);

	ICUStrftime::AddBinaryTimestampFunction("strftime", context);

	// Add string casts
	ICUStrptime::AddCasts(context);
	ICUStrftime::AddCasts(context);
}

} // namespace duckdb
