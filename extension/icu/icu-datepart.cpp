#include "include/icu-datepart.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDatePart : public ICUDateFunc {
	typedef int64_t (*part_adapter_t)(icu::Calendar *calendar, const uint64_t micros);

	// Date part adapters
	static int64_t ExtractEra(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_ERA);
	}

	static int64_t ExtractYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_YEAR);
	}

	static int64_t ExtractDecade(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) / 10;
	}

	static int64_t ExtractCentury(icu::Calendar *calendar, const uint64_t micros) {
		const auto era = ExtractEra(calendar, micros);
		const auto cccc = ((ExtractYear(calendar, micros) - 1) / 100) + 1;
		return era > 0 ? cccc : -cccc;
	}

	static int64_t ExtractMillenium(icu::Calendar *calendar, const uint64_t micros) {
		const auto era = ExtractEra(calendar, micros);
		const auto mmmm = ((ExtractYear(calendar, micros) - 1) / 1000) + 1;
		return era > 0 ? mmmm : -mmmm;
	}

	static int64_t ExtractMonth(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) + 1;
	}

	static int64_t ExtractQuarter(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) / Interval::MONTHS_PER_QUARTER + 1;
	}

	static int64_t ExtractDay(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DATE);
	}

	static int64_t ExtractDayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK) - UCAL_SUNDAY;
	}

	static int64_t ExtractISODayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK);
	}

	static int64_t ExtractWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		return ExtractField(calendar, UCAL_WEEK_OF_YEAR);
	}

	static int64_t ExtractISOYear(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		return ExtractField(calendar, UCAL_YEAR_WOY);
	}

	static int64_t ExtractYearWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		const auto iyyy = ExtractField(calendar, UCAL_YEAR_WOY);
		const auto ww = ExtractField(calendar, UCAL_WEEK_OF_YEAR);
		return iyyy * 100 + ((iyyy > 0) ? ww : -ww);
	}

	static int64_t ExtractDayOfYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DAY_OF_YEAR);
	}

	static int64_t ExtractHour(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_HOUR_OF_DAY);
	}

	static int64_t ExtractMinute(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MINUTE);
	}

	static int64_t ExtractSecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_SECOND);
	}

	static int64_t ExtractMillisecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractSecond(calendar, micros) * Interval::MSECS_PER_SEC + ExtractField(calendar, UCAL_MILLISECOND);
	}

	static int64_t ExtractMicrosecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractMillisecond(calendar, micros) * Interval::MICROS_PER_MSEC + micros;
	}

	static int64_t ExtractEpoch(icu::Calendar *calendar, const uint64_t micros) {
		UErrorCode status = U_ZERO_ERROR;
		auto millis = calendar->getTime(status);
		millis += ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis += ExtractField(calendar, UCAL_DST_OFFSET);
		//	Truncate
		return millis / Interval::MSECS_PER_SEC;
	}

	static int64_t ExtractTimezone(icu::Calendar *calendar, const uint64_t micros) {
		auto millis = ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis += ExtractField(calendar, UCAL_DST_OFFSET);
		return millis / Interval::MSECS_PER_SEC;
	}

	static int64_t ExtractTimezoneHour(icu::Calendar *calendar, const uint64_t micros) {
		auto secs = ExtractTimezone(calendar, micros);
		return secs / Interval::SECS_PER_HOUR;
	}

	static int64_t ExtractTimezoneMinute(icu::Calendar *calendar, const uint64_t micros) {
		auto secs = ExtractTimezone(calendar, micros);
		return (secs % Interval::SECS_PER_HOUR) / Interval::SECS_PER_MINUTE;
	}

	static part_adapter_t PartCodeAdapterFactory(DatePartSpecifier part) {
		switch (part) {
		case DatePartSpecifier::YEAR:
			return ExtractYear;
		case DatePartSpecifier::MONTH:
			return ExtractMonth;
		case DatePartSpecifier::DAY:
			return ExtractDay;
		case DatePartSpecifier::DECADE:
			return ExtractDecade;
		case DatePartSpecifier::CENTURY:
			return ExtractCentury;
		case DatePartSpecifier::MILLENNIUM:
			return ExtractMillenium;
		case DatePartSpecifier::MICROSECONDS:
			return ExtractMicrosecond;
		case DatePartSpecifier::MILLISECONDS:
			return ExtractMillisecond;
		case DatePartSpecifier::SECOND:
			return ExtractSecond;
		case DatePartSpecifier::MINUTE:
			return ExtractMinute;
		case DatePartSpecifier::HOUR:
			return ExtractHour;
		case DatePartSpecifier::DOW:
			return ExtractDayOfWeek;
		case DatePartSpecifier::ISODOW:
			return ExtractISODayOfWeek;
		case DatePartSpecifier::WEEK:
			return ExtractWeek;
		case DatePartSpecifier::ISOYEAR:
			return ExtractISOYear;
		case DatePartSpecifier::DOY:
			return ExtractDayOfYear;
		case DatePartSpecifier::QUARTER:
			return ExtractQuarter;
		case DatePartSpecifier::YEARWEEK:
			return ExtractYearWeek;
		case DatePartSpecifier::EPOCH:
			return ExtractEpoch;
		case DatePartSpecifier::ERA:
			return ExtractEra;
		case DatePartSpecifier::TIMEZONE:
			return ExtractTimezone;
		case DatePartSpecifier::TIMEZONE_HOUR:
			return ExtractTimezoneHour;
		case DatePartSpecifier::TIMEZONE_MINUTE:
			return ExtractTimezoneMinute;
		default:
			throw Exception("Unsupported ICU extract adapter");
		}
	}

	static date_t MakeLastDay(icu::Calendar *calendar, const uint64_t micros) {
		// Set the calendar to midnight on the last day of the month
		calendar->set(UCAL_MILLISECOND, 0);
		calendar->set(UCAL_SECOND, 0);
		calendar->set(UCAL_MINUTE, 0);
		calendar->set(UCAL_HOUR_OF_DAY, 0);

		UErrorCode status = U_ZERO_ERROR;
		const auto dd = calendar->getActualMaximum(UCAL_DATE, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to extract ICU last day.");
		}

		calendar->set(UCAL_DATE, dd);

		return Date::EpochToDate(ExtractEpoch(calendar, 0));
	}

	template <typename RESULT_TYPE>
	struct BindAdapterData : public BindData {
		using result_t = RESULT_TYPE;
		typedef result_t (*adapter_t)(icu::Calendar *calendar, const uint64_t micros);
		using adapters_t = vector<adapter_t>;

		BindAdapterData(ClientContext &context, adapter_t adapter_p) : BindData(context), adapters(1, adapter_p) {
		}
		BindAdapterData(ClientContext &context, adapters_t &adapters_p) : BindData(context), adapters(adapters_p) {
		}
		BindAdapterData(const BindAdapterData &other) : BindData(other), adapters(other.adapters) {
		}

		adapters_t adapters;

		bool Equals(const FunctionData &other_p) const override {
			const auto &other = (BindAdapterData &)other_p;
			return BindData::Equals(other_p) && adapters == other.adapters;
		}

		unique_ptr<FunctionData> Copy() const override {
			return make_unique<BindAdapterData>(*this);
		}
	};

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static void UnaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindAdapterData<RESULT_TYPE>;
		D_ASSERT(args.ColumnCount() == 1);
		auto &date_arg = args.data[0];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BIND_TYPE &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		UnaryExecutor::ExecuteWithNulls<INPUT_TYPE, RESULT_TYPE>(date_arg, result, args.size(),
		                                                         [&](INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
			                                                         if (Timestamp::IsFinite(input)) {
				                                                         const auto micros = SetTime(calendar, input);
				                                                         return info.adapters[0](calendar, micros);
			                                                         } else {
				                                                         mask.SetInvalid(idx);
				                                                         return RESULT_TYPE(0);
			                                                         }
		                                                         });
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static void BinaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindAdapterData<int64_t>;
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BIND_TYPE &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		BinaryExecutor::ExecuteWithNulls<string_t, INPUT_TYPE, RESULT_TYPE>(
		    part_arg, date_arg, result, args.size(),
		    [&](string_t specifier, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
			    if (Timestamp::IsFinite(input)) {
				    const auto micros = SetTime(calendar, input);
				    auto adapter = PartCodeAdapterFactory(GetDatePartSpecifier(specifier.GetString()));
				    return adapter(calendar, micros);
			    } else {
				    mask.SetInvalid(idx);
				    return RESULT_TYPE(0);
			    }
		    });
	}

	template <typename INPUT_TYPE>
	static void StructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindAdapterData<int64_t>;
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BIND_TYPE &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		D_ASSERT(args.ColumnCount() == 1);
		const auto count = args.size();
		Vector &input = args.data[0];

		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				auto tdata = ConstantVector::GetData<INPUT_TYPE>(input);
				auto micros = SetTime(calendar, tdata[0]);
				const auto is_finite = Timestamp::IsFinite(*tdata);
				auto &child_entries = StructVector::GetEntries(result);
				for (size_t col = 0; col < child_entries.size(); ++col) {
					auto &child_entry = child_entries[col];
					if (is_finite) {
						ConstantVector::SetNull(*child_entry, false);
						auto pdata = ConstantVector::GetData<int64_t>(*child_entry);
						auto adapter = info.adapters[col];
						pdata[0] = adapter(calendar, micros);
					} else {
						ConstantVector::SetNull(*child_entry, true);
					}
				}
			}
		} else {
			UnifiedVectorFormat rdata;
			input.ToUnifiedFormat(count, rdata);

			const auto &arg_valid = rdata.validity;
			auto tdata = (const INPUT_TYPE *)rdata.data;

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &child_entries = StructVector::GetEntries(result);
			for (auto &child_entry : child_entries) {
				child_entry->SetVectorType(VectorType::FLAT_VECTOR);
			}

			auto &res_valid = FlatVector::Validity(result);
			for (idx_t i = 0; i < count; ++i) {
				const auto idx = rdata.sel->get_index(i);
				if (arg_valid.RowIsValid(idx)) {
					res_valid.SetValid(idx);
					auto micros = SetTime(calendar, tdata[idx]);
					const auto is_finite = Timestamp::IsFinite(tdata[idx]);
					for (size_t col = 0; col < child_entries.size(); ++col) {
						auto &child_entry = child_entries[col];
						if (is_finite) {
							FlatVector::Validity(*child_entry).SetValid(idx);
							auto pdata = FlatVector::GetData<int64_t>(*child_entry);
							auto adapter = info.adapters[col];
							pdata[idx] = adapter(calendar, micros);
						} else {
							FlatVector::Validity(*child_entry).SetInvalid(idx);
						}
					}
				} else {
					res_valid.SetInvalid(idx);
					for (auto &child_entry : child_entries) {
						FlatVector::Validity(*child_entry).SetInvalid(idx);
					}
				}
			}
		}

		result.Verify(count);
	}

	template <typename BIND_TYPE>
	static unique_ptr<FunctionData> BindAdapter(ClientContext &context, ScalarFunction &bound_function,
	                                            vector<unique_ptr<Expression>> &arguments,
	                                            typename BIND_TYPE::adapter_t adapter) {
		return make_unique<BIND_TYPE>(context, adapter);
	}

	static unique_ptr<FunctionData> BindDatePart(ClientContext &context, ScalarFunction &bound_function,
	                                             vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<int64_t>;
		auto adapter =
		    (arguments.size() == 1) ? PartCodeAdapterFactory(GetDatePartSpecifier(bound_function.name)) : nullptr;
		return BindAdapter<data_t>(context, bound_function, arguments, adapter);
	}

	static unique_ptr<FunctionData> BindStruct(ClientContext &context, ScalarFunction &bound_function,
	                                           vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<int64_t>;
		using adapters_t = data_t::adapters_t;

		// collect names and deconflict, construct return type
		if (arguments[0]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[0]->IsFoldable()) {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		case_insensitive_set_t name_collision_set;
		child_list_t<LogicalType> struct_children;
		adapters_t adapters;

		Value parts_list = ExpressionExecutor::EvaluateScalar(*arguments[0]);
		if (parts_list.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(parts_list);
			if (list_children.empty()) {
				throw BinderException("%s requires non-empty lists of part names", bound_function.name);
			}
			for (const auto &part_value : list_children) {
				if (part_value.IsNull()) {
					throw BinderException("NULL struct entry name in %s", bound_function.name);
				}
				const auto part_name = part_value.ToString();
				const auto part_code = GetDatePartSpecifier(part_name);
				if (name_collision_set.find(part_name) != name_collision_set.end()) {
					throw BinderException("Duplicate struct entry name \"%s\" in %s", part_name, bound_function.name);
				}
				name_collision_set.insert(part_name);
				adapters.emplace_back(PartCodeAdapterFactory(part_code));
				struct_children.emplace_back(make_pair(part_name, LogicalType::BIGINT));
			}
		} else {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		arguments.erase(arguments.begin());
		bound_function.arguments.erase(bound_function.arguments.begin());
		bound_function.return_type = LogicalType::STRUCT(move(struct_children));
		return make_unique<data_t>(context, adapters);
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static ScalarFunction GetUnaryPartCodeFunction(const LogicalType &temporal_type) {
		return ScalarFunction({temporal_type}, LogicalType::BIGINT, UnaryTimestampFunction<INPUT_TYPE, RESULT_TYPE>,
		                      BindDatePart);
	}

	static void AddUnaryPartCodeFunctions(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunctionSet set(name);
		set.AddFunction(GetUnaryPartCodeFunction<timestamp_t, int64_t>(LogicalType::TIMESTAMP_TZ));
		CreateScalarFunctionInfo func_info(set);
		catalog.AddFunction(context, &func_info);
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static ScalarFunction GetBinaryPartCodeFunction(const LogicalType &temporal_type) {
		return ScalarFunction({LogicalType::VARCHAR, temporal_type}, LogicalType::BIGINT,
		                      BinaryTimestampFunction<INPUT_TYPE, RESULT_TYPE>, BindDatePart);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetStructFunction(const LogicalType &temporal_type) {
		auto part_type = LogicalType::LIST(LogicalType::VARCHAR);
		auto result_type = LogicalType::STRUCT({});
		return ScalarFunction({part_type, temporal_type}, result_type, StructFunction<INPUT_TYPE>, BindStruct);
	}

	static void AddDatePartFunctions(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunctionSet set(name);
		set.AddFunction(GetBinaryPartCodeFunction<timestamp_t, int64_t>(LogicalType::TIMESTAMP_TZ));
		set.AddFunction(GetStructFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		CreateScalarFunctionInfo func_info(set);
		catalog.AddFunction(context, &func_info);
	}

	static unique_ptr<FunctionData> BindLastDate(ClientContext &context, ScalarFunction &bound_function,
	                                             vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<date_t>;
		return BindAdapter<data_t>(context, bound_function, arguments, MakeLastDay);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetLastDayFunction(const LogicalType &temporal_type) {
		return ScalarFunction({temporal_type}, LogicalType::DATE, UnaryTimestampFunction<INPUT_TYPE, date_t>,
		                      BindLastDate);
	}
	static void AddLastDayFunctions(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunctionSet set(name);
		set.AddFunction(GetLastDayFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		CreateScalarFunctionInfo func_info(set);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDatePartFunctions(ClientContext &context) {
	// register the individual operators
	ICUDatePart::AddUnaryPartCodeFunctions("era", context);
	ICUDatePart::AddUnaryPartCodeFunctions("year", context);
	ICUDatePart::AddUnaryPartCodeFunctions("month", context);
	ICUDatePart::AddUnaryPartCodeFunctions("day", context);
	ICUDatePart::AddUnaryPartCodeFunctions("decade", context);
	ICUDatePart::AddUnaryPartCodeFunctions("century", context);
	ICUDatePart::AddUnaryPartCodeFunctions("millennium", context);
	ICUDatePart::AddUnaryPartCodeFunctions("microsecond", context);
	ICUDatePart::AddUnaryPartCodeFunctions("millisecond", context);
	ICUDatePart::AddUnaryPartCodeFunctions("second", context);
	ICUDatePart::AddUnaryPartCodeFunctions("minute", context);
	ICUDatePart::AddUnaryPartCodeFunctions("hour", context);
	ICUDatePart::AddUnaryPartCodeFunctions("dayofweek", context);
	ICUDatePart::AddUnaryPartCodeFunctions("isodow", context);
	ICUDatePart::AddUnaryPartCodeFunctions("week", context); //  Note that WeekOperator is ISO-8601, not US
	ICUDatePart::AddUnaryPartCodeFunctions("dayofyear", context);
	ICUDatePart::AddUnaryPartCodeFunctions("quarter", context);
	ICUDatePart::AddUnaryPartCodeFunctions("epoch", context);
	ICUDatePart::AddUnaryPartCodeFunctions("isoyear", context);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone", context);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone_hour", context);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone_minute", context);

	//  register combinations
	ICUDatePart::AddUnaryPartCodeFunctions("yearweek", context); //  Note this is ISO year and week

	//  register various aliases
	ICUDatePart::AddUnaryPartCodeFunctions("dayofmonth", context);
	ICUDatePart::AddUnaryPartCodeFunctions("weekday", context);
	ICUDatePart::AddUnaryPartCodeFunctions("weekofyear", context);

	//  register the last_day function
	ICUDatePart::AddLastDayFunctions("last_day", context);

	// finally the actual date_part function
	ICUDatePart::AddDatePartFunctions("date_part", context);
	ICUDatePart::AddDatePartFunctions("datepart", context);
}

} // namespace duckdb
