#include "include/icu-datepart.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/main/extension_util.hpp"
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
	typedef int64_t (*part_bigint_t)(icu::Calendar *calendar, const uint64_t micros);
	typedef double (*part_double_t)(icu::Calendar *calendar, const uint64_t micros);

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
		// [Sun(0), Sat(6)]
		return ExtractField(calendar, UCAL_DAY_OF_WEEK) - UCAL_SUNDAY;
	}

	static int64_t ExtractISODayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		// [Mon(1), Sun(7)]
		return 1 + (ExtractField(calendar, UCAL_DAY_OF_WEEK) + 7 - UCAL_MONDAY) % 7;
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

	static double ExtractEpoch(icu::Calendar *calendar, const uint64_t micros) {
		UErrorCode status = U_ZERO_ERROR;
		auto result = calendar->getTime(status) / Interval::MSECS_PER_SEC;
		result += micros / double(Interval::MICROS_PER_SEC);
		return result;
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

	//	PG uses doubles for JDs so we can only use them with other double types
	static double ExtractJulianDay(icu::Calendar *calendar, const uint64_t micros) {
		//	We need days + fraction
		auto days = ExtractField(calendar, UCAL_JULIAN_DAY);
		auto frac = ExtractHour(calendar, micros);

		frac *= Interval::MINS_PER_HOUR;
		frac += ExtractMinute(calendar, micros);

		frac *= Interval::MICROS_PER_MINUTE;
		frac += ExtractMicrosecond(calendar, micros);

		double result = frac;
		result /= Interval::MICROS_PER_DAY;
		result += days;

		return result;
	}

	static part_bigint_t PartCodeBigintFactory(DatePartSpecifier part) {
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
		case DatePartSpecifier::ERA:
			return ExtractEra;
		case DatePartSpecifier::TIMEZONE:
			return ExtractTimezone;
		case DatePartSpecifier::TIMEZONE_HOUR:
			return ExtractTimezoneHour;
		case DatePartSpecifier::TIMEZONE_MINUTE:
			return ExtractTimezoneMinute;
		default:
			throw InternalException("Unsupported ICU BIGINT extractor");
		}
	}

	static part_double_t PartCodeDoubleFactory(DatePartSpecifier part) {
		switch (part) {
		case DatePartSpecifier::EPOCH:
			return ExtractEpoch;
		case DatePartSpecifier::JULIAN_DAY:
			return ExtractJulianDay;
		default:
			throw InternalException("Unsupported ICU DOUBLE extractor");
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
			throw InternalException("Unable to extract ICU last day.");
		}

		calendar->set(UCAL_DATE, dd);

		//	Offset to UTC
		auto millis = calendar->getTime(status);
		millis += ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis += ExtractField(calendar, UCAL_DST_OFFSET);

		return Date::EpochToDate(millis / Interval::MSECS_PER_SEC);
	}

	static string_t MonthName(icu::Calendar *calendar, const uint64_t micros) {
		const auto mm = ExtractMonth(calendar, micros) - 1;
		if (mm == 12) {
			return "Undecimber";
		}
		return Date::MONTH_NAMES[mm];
	}

	static string_t DayName(icu::Calendar *calendar, const uint64_t micros) {
		return Date::DAY_NAMES[ExtractDayOfWeek(calendar, micros)];
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
			const auto &other = other_p.Cast<BindAdapterData>();
			return BindData::Equals(other_p) && adapters == other.adapters;
		}

		duckdb::unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindAdapterData>(*this);
		}
	};

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static void UnaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindAdapterData<RESULT_TYPE>;
		D_ASSERT(args.ColumnCount() == 1);
		auto &date_arg = args.data[0];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BIND_TYPE>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		UnaryExecutor::ExecuteWithNulls<INPUT_TYPE, RESULT_TYPE>(date_arg, result, args.size(),
		                                                         [&](INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
			                                                         if (Timestamp::IsFinite(input)) {
				                                                         const auto micros = SetTime(calendar, input);
				                                                         return info.adapters[0](calendar, micros);
			                                                         } else {
				                                                         mask.SetInvalid(idx);
				                                                         return RESULT_TYPE();
			                                                         }
		                                                         });
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static void BinaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindAdapterData<int64_t>;
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BIND_TYPE>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		BinaryExecutor::ExecuteWithNulls<string_t, INPUT_TYPE, RESULT_TYPE>(
		    part_arg, date_arg, result, args.size(),
		    [&](string_t specifier, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
			    if (Timestamp::IsFinite(input)) {
				    const auto micros = SetTime(calendar, input);
				    auto adapter = PartCodeBigintFactory(GetDatePartSpecifier(specifier.GetString()));
				    return adapter(calendar, micros);
			    } else {
				    mask.SetInvalid(idx);
				    return RESULT_TYPE(0);
			    }
		    });
	}

	struct BindStructData : public BindData {
		using part_codes_t = vector<DatePartSpecifier>;
		using bigints_t = vector<part_bigint_t>;
		using doubles_t = vector<part_double_t>;

		BindStructData(ClientContext &context, part_codes_t &&part_codes_p)
		    : BindData(context), part_codes(part_codes_p) {
			InitFactories();
		}
		BindStructData(const string &tz_setting_p, const string &cal_setting_p, part_codes_t &&part_codes_p)
		    : BindData(tz_setting_p, cal_setting_p), part_codes(part_codes_p) {
			InitFactories();
		}
		BindStructData(const BindStructData &other)
		    : BindData(other), part_codes(other.part_codes), bigints(other.bigints), doubles(other.doubles) {
		}

		part_codes_t part_codes;
		bigints_t bigints;
		doubles_t doubles;

		bool Equals(const FunctionData &other_p) const override {
			const auto &other = other_p.Cast<BindStructData>();
			return BindData::Equals(other_p) && part_codes == other.part_codes;
		}

		duckdb::unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindStructData>(*this);
		}

		void InitFactories() {
			bigints.clear();
			bigints.resize(part_codes.size(), nullptr);
			doubles.clear();
			doubles.resize(part_codes.size(), nullptr);
			for (size_t col = 0; col < part_codes.size(); ++col) {
				const auto part_code = part_codes[col];
				if (IsBigintDatepart(part_code)) {
					bigints[col] = PartCodeBigintFactory(part_code);
				} else {
					doubles[col] = PartCodeDoubleFactory(part_code);
				}
			}
		}
	};

	template <typename INPUT_TYPE>
	static void StructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindStructData>();
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
						if (IsBigintDatepart(info.part_codes[col])) {
							auto pdata = ConstantVector::GetData<int64_t>(*child_entry);
							auto adapter = info.bigints[col];
							pdata[0] = adapter(calendar, micros);
						} else {
							auto pdata = ConstantVector::GetData<double>(*child_entry);
							auto adapter = info.doubles[col];
							pdata[0] = adapter(calendar, micros);
						}
					} else {
						ConstantVector::SetNull(*child_entry, true);
					}
				}
			}
		} else {
			UnifiedVectorFormat rdata;
			input.ToUnifiedFormat(count, rdata);

			const auto &arg_valid = rdata.validity;
			auto tdata = UnifiedVectorFormat::GetData<INPUT_TYPE>(rdata);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &child_entries = StructVector::GetEntries(result);
			for (auto &child_entry : child_entries) {
				child_entry->SetVectorType(VectorType::FLAT_VECTOR);
			}

			auto &res_valid = FlatVector::Validity(result);
			for (idx_t i = 0; i < count; ++i) {
				const auto idx = rdata.sel->get_index(i);
				if (arg_valid.RowIsValid(idx)) {
					res_valid.SetValid(i);
					auto micros = SetTime(calendar, tdata[idx]);
					const auto is_finite = Timestamp::IsFinite(tdata[idx]);
					for (size_t col = 0; col < child_entries.size(); ++col) {
						auto &child_entry = child_entries[col];
						if (is_finite) {
							FlatVector::Validity(*child_entry).SetValid(i);
							if (IsBigintDatepart(info.part_codes[col])) {
								auto pdata = ConstantVector::GetData<int64_t>(*child_entry);
								auto adapter = info.bigints[col];
								pdata[i] = adapter(calendar, micros);
							} else {
								auto pdata = ConstantVector::GetData<double>(*child_entry);
								auto adapter = info.doubles[col];
								pdata[i] = adapter(calendar, micros);
							}
						} else {
							FlatVector::Validity(*child_entry).SetInvalid(i);
						}
					}
				} else {
					res_valid.SetInvalid(i);
					for (auto &child_entry : child_entries) {
						FlatVector::Validity(*child_entry).SetInvalid(i);
					}
				}
			}
		}

		result.Verify(count);
	}

	template <typename BIND_TYPE>
	static duckdb::unique_ptr<FunctionData> BindAdapter(ClientContext &context, ScalarFunction &bound_function,
	                                                    vector<duckdb::unique_ptr<Expression>> &arguments,
	                                                    typename BIND_TYPE::adapter_t adapter) {
		return make_uniq<BIND_TYPE>(context, adapter);
	}

	static duckdb::unique_ptr<FunctionData> BindUnaryDatePart(ClientContext &context, ScalarFunction &bound_function,
	                                                          vector<duckdb::unique_ptr<Expression>> &arguments) {
		const auto part_code = GetDatePartSpecifier(bound_function.name);
		if (IsBigintDatepart(part_code)) {
			using data_t = BindAdapterData<int64_t>;
			auto adapter = PartCodeBigintFactory(part_code);
			return BindAdapter<data_t>(context, bound_function, arguments, adapter);
		} else {
			using data_t = BindAdapterData<double>;
			auto adapter = PartCodeDoubleFactory(part_code);
			return BindAdapter<data_t>(context, bound_function, arguments, adapter);
		}
	}

	static duckdb::unique_ptr<FunctionData> BindBinaryDatePart(ClientContext &context, ScalarFunction &bound_function,
	                                                           vector<duckdb::unique_ptr<Expression>> &arguments) {
		//	If we are only looking for Julian Days, then patch in the unary function.
		do {
			if (arguments[0]->HasParameter() || !arguments[0]->IsFoldable()) {
				break;
			}

			Value part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
			if (part_value.IsNull()) {
				break;
			}

			const auto part_name = part_value.ToString();
			const auto part_code = GetDatePartSpecifier(part_name);
			if (IsBigintDatepart(part_code)) {
				break;
			}

			arguments.erase(arguments.begin());
			bound_function.arguments.erase(bound_function.arguments.begin());
			bound_function.name = part_name;
			bound_function.return_type = LogicalType::DOUBLE;
			bound_function.function = UnaryTimestampFunction<timestamp_t, double>;

			return BindUnaryDatePart(context, bound_function, arguments);
		} while (false);

		using data_t = BindAdapterData<int64_t>;
		return BindAdapter<data_t>(context, bound_function, arguments, nullptr);
	}

	static duckdb::unique_ptr<FunctionData> BindStruct(ClientContext &context, ScalarFunction &bound_function,
	                                                   vector<duckdb::unique_ptr<Expression>> &arguments) {
		// collect names and deconflict, construct return type
		if (arguments[0]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[0]->IsFoldable()) {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		case_insensitive_set_t name_collision_set;
		child_list_t<LogicalType> struct_children;
		BindStructData::part_codes_t part_codes;

		Value parts_list = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		if (parts_list.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(parts_list);
			if (list_children.empty()) {
				throw BinderException("%s requires non-empty lists of part names", bound_function.name);
			}

			for (size_t col = 0; col < list_children.size(); ++col) {
				const auto &part_value = list_children[col];
				if (part_value.IsNull()) {
					throw BinderException("NULL struct entry name in %s", bound_function.name);
				}
				const auto part_name = part_value.ToString();
				const auto part_code = GetDatePartSpecifier(part_name);
				if (name_collision_set.find(part_name) != name_collision_set.end()) {
					throw BinderException("Duplicate struct entry name \"%s\" in %s", part_name, bound_function.name);
				}
				name_collision_set.insert(part_name);
				part_codes.emplace_back(part_code);
				if (IsBigintDatepart(part_code)) {
					struct_children.emplace_back(make_pair(part_name, LogicalType::BIGINT));
				} else {
					struct_children.emplace_back(make_pair(part_name, LogicalType::DOUBLE));
				}
			}
		} else {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		Function::EraseArgument(bound_function, arguments, 0);
		bound_function.return_type = LogicalType::STRUCT(std::move(struct_children));
		return make_uniq<BindStructData>(context, std::move(part_codes));
	}

	static void SerializeStructFunction(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                                    const ScalarFunction &function) {
		D_ASSERT(bind_data);
		auto &info = bind_data->Cast<BindStructData>();
		serializer.WriteProperty(100, "tz_setting", info.tz_setting);
		serializer.WriteProperty(101, "cal_setting", info.cal_setting);
		serializer.WriteProperty(102, "part_codes", info.part_codes);
	}

	static duckdb::unique_ptr<FunctionData> DeserializeStructFunction(Deserializer &deserializer,
	                                                                  ScalarFunction &bound_function) {
		auto tz_setting = deserializer.ReadProperty<string>(100, "tz_setting");
		auto cal_setting = deserializer.ReadProperty<string>(101, "cal_setting");
		auto part_codes = deserializer.ReadProperty<vector<DatePartSpecifier>>(102, "part_codes");
		return make_uniq<BindStructData>(tz_setting, cal_setting, std::move(part_codes));
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static ScalarFunction GetUnaryPartCodeFunction(const LogicalType &temporal_type,
	                                               const LogicalType &result_type = LogicalType::BIGINT) {
		return ScalarFunction({temporal_type}, result_type, UnaryTimestampFunction<INPUT_TYPE, RESULT_TYPE>,
		                      BindUnaryDatePart);
	}

	template <typename RESULT_TYPE = int64_t>
	static void AddUnaryPartCodeFunctions(const string &name, DatabaseInstance &db,
	                                      const LogicalType &result_type = LogicalType::BIGINT) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetUnaryPartCodeFunction<timestamp_t, RESULT_TYPE>(LogicalType::TIMESTAMP_TZ, result_type));
		ExtensionUtil::RegisterFunction(db, set);
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static ScalarFunction GetBinaryPartCodeFunction(const LogicalType &temporal_type) {
		return ScalarFunction({LogicalType::VARCHAR, temporal_type}, LogicalType::BIGINT,
		                      BinaryTimestampFunction<INPUT_TYPE, RESULT_TYPE>, BindBinaryDatePart);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetStructFunction(const LogicalType &temporal_type) {
		auto part_type = LogicalType::LIST(LogicalType::VARCHAR);
		auto result_type = LogicalType::STRUCT({});
		ScalarFunction result({part_type, temporal_type}, result_type, StructFunction<INPUT_TYPE>, BindStruct);
		result.serialize = SerializeStructFunction;
		result.deserialize = DeserializeStructFunction;
		return result;
	}

	static void AddDatePartFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetBinaryPartCodeFunction<timestamp_t, int64_t>(LogicalType::TIMESTAMP_TZ));
		set.AddFunction(GetStructFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		for (auto &func : set.functions) {
			BaseScalarFunction::SetReturnsError(func);
		}
		ExtensionUtil::RegisterFunction(db, set);
	}

	static duckdb::unique_ptr<FunctionData> BindLastDate(ClientContext &context, ScalarFunction &bound_function,
	                                                     vector<duckdb::unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<date_t>;
		return BindAdapter<data_t>(context, bound_function, arguments, MakeLastDay);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetLastDayFunction(const LogicalType &temporal_type) {
		return ScalarFunction({temporal_type}, LogicalType::DATE, UnaryTimestampFunction<INPUT_TYPE, date_t>,
		                      BindLastDate);
	}
	static void AddLastDayFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetLastDayFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		ExtensionUtil::RegisterFunction(db, set);
	}

	static unique_ptr<FunctionData> BindMonthName(ClientContext &context, ScalarFunction &bound_function,
	                                              vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<string_t>;
		return BindAdapter<data_t>(context, bound_function, arguments, MonthName);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetMonthNameFunction(const LogicalType &temporal_type) {
		return ScalarFunction({temporal_type}, LogicalType::VARCHAR, UnaryTimestampFunction<INPUT_TYPE, string_t>,
		                      BindMonthName);
	}
	static void AddMonthNameFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetMonthNameFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		ExtensionUtil::RegisterFunction(db, set);
	}

	static unique_ptr<FunctionData> BindDayName(ClientContext &context, ScalarFunction &bound_function,
	                                            vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindAdapterData<string_t>;
		return BindAdapter<data_t>(context, bound_function, arguments, DayName);
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetDayNameFunction(const LogicalType &temporal_type) {
		return ScalarFunction({temporal_type}, LogicalType::VARCHAR, UnaryTimestampFunction<INPUT_TYPE, string_t>,
		                      BindDayName);
	}
	static void AddDayNameFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetDayNameFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		ExtensionUtil::RegisterFunction(db, set);
	}
};

void RegisterICUDatePartFunctions(DatabaseInstance &db) {
	// register the individual operators

	//	BIGINTs
	ICUDatePart::AddUnaryPartCodeFunctions("era", db);
	ICUDatePart::AddUnaryPartCodeFunctions("year", db);
	ICUDatePart::AddUnaryPartCodeFunctions("month", db);
	ICUDatePart::AddUnaryPartCodeFunctions("day", db);
	ICUDatePart::AddUnaryPartCodeFunctions("decade", db);
	ICUDatePart::AddUnaryPartCodeFunctions("century", db);
	ICUDatePart::AddUnaryPartCodeFunctions("millennium", db);
	ICUDatePart::AddUnaryPartCodeFunctions("microsecond", db);
	ICUDatePart::AddUnaryPartCodeFunctions("millisecond", db);
	ICUDatePart::AddUnaryPartCodeFunctions("second", db);
	ICUDatePart::AddUnaryPartCodeFunctions("minute", db);
	ICUDatePart::AddUnaryPartCodeFunctions("hour", db);
	ICUDatePart::AddUnaryPartCodeFunctions("dayofweek", db);
	ICUDatePart::AddUnaryPartCodeFunctions("isodow", db);
	ICUDatePart::AddUnaryPartCodeFunctions("week", db); //  Note that WeekOperator is ISO-8601, not US
	ICUDatePart::AddUnaryPartCodeFunctions("dayofyear", db);
	ICUDatePart::AddUnaryPartCodeFunctions("quarter", db);
	ICUDatePart::AddUnaryPartCodeFunctions("isoyear", db);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone", db);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone_hour", db);
	ICUDatePart::AddUnaryPartCodeFunctions("timezone_minute", db);

	//	DOUBLEs
	ICUDatePart::AddUnaryPartCodeFunctions<double>("epoch", db, LogicalType::DOUBLE);
	ICUDatePart::AddUnaryPartCodeFunctions<double>("julian", db, LogicalType::DOUBLE);

	//  register combinations
	ICUDatePart::AddUnaryPartCodeFunctions("yearweek", db); //  Note this is ISO year and week

	//  register various aliases
	ICUDatePart::AddUnaryPartCodeFunctions("dayofmonth", db);
	ICUDatePart::AddUnaryPartCodeFunctions("weekday", db);
	ICUDatePart::AddUnaryPartCodeFunctions("weekofyear", db);

	//  register the last_day function
	ICUDatePart::AddLastDayFunctions("last_day", db);

	// register the dayname/monthname functions
	ICUDatePart::AddMonthNameFunctions("monthname", db);
	ICUDatePart::AddDayNameFunctions("dayname", db);

	// finally the actual date_part function
	ICUDatePart::AddDatePartFunctions("date_part", db);
	ICUDatePart::AddDatePartFunctions("datepart", db);
}

} // namespace duckdb
