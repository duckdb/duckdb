#include "icu-extension.hpp"
#include "icu-collate.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog.hpp"

#include <cassert>

namespace duckdb {

struct IcuBindData : public FunctionData {
	std::unique_ptr<icu::Collator> collator;
	string language;
	string country;

	IcuBindData(string language_p, string country_p) : language(move(language_p)), country(move(country_p)) {
		UErrorCode status = U_ZERO_ERROR;
		this->collator = std::unique_ptr<icu::Collator>(
		    icu::Collator::createInstance(icu::Locale(language.c_str(), country.c_str()), status));
		if (U_FAILURE(status)) {
			throw Exception("Failed to create ICU collator!");
		}
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<IcuBindData>(language.c_str(), country.c_str());
	}
};

static int32_t ICUGetSortKey(icu::Collator &collator, string_t input, unique_ptr<char[]> &buffer,
                             int32_t &buffer_size) {
	int32_t string_size =
	    collator.getSortKey(icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetDataUnsafe(), input.GetSize())),
	                        (uint8_t *)buffer.get(), buffer_size);
	if (string_size > buffer_size) {
		// have to resize the buffer
		buffer_size = string_size;
		buffer = unique_ptr<char[]>(new char[buffer_size]);

		string_size =
		    collator.getSortKey(icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetDataUnsafe(), input.GetSize())),
		                        (uint8_t *)buffer.get(), buffer_size);
	}
	return string_size;
}

static void ICUCollateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const char HEX_TABLE[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IcuBindData &)*func_expr.bind_info;
	auto &collator = *info.collator;

	unique_ptr<char[]> buffer;
	int32_t buffer_size = 0;
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		// create a sort key from the string
		int32_t string_size = ICUGetSortKey(collator, input, buffer, buffer_size);
		// convert the sort key to hexadecimal
		auto str_result = StringVector::EmptyString(result, (string_size - 1) * 2);
		auto str_data = str_result.GetDataWriteable();
		for (idx_t i = 0; i < string_size - 1; i++) {
			uint8_t byte = uint8_t(buffer[i]);
			assert(byte != 0);
			str_data[i * 2] = HEX_TABLE[byte / 16];
			str_data[i * 2 + 1] = HEX_TABLE[byte % 16];
		}
		// printf("%s: %s\n", input.GetString().c_str(), str_result.GetString().c_str());
		return str_result;
	});
}

static unique_ptr<FunctionData> ICUCollateBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto splits = StringUtil::Split(bound_function.name, "_");
	if (splits.size() == 1) {
		return make_unique<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_unique<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InternalException("Expected one or two splits");
	}
}

static unique_ptr<FunctionData> ICUSortKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsFoldable()) {
		throw NotImplementedException("ICU_SORT_KEY(VARCHAR, VARCHAR) with non-constant collation is not supported");
	}
	Value val = ExpressionExecutor::EvaluateScalar(*arguments[1]).CastAs(LogicalType::VARCHAR);
	if (val.is_null) {
		throw NotImplementedException("ICU_SORT_KEY(VARCHAR, VARCHAR) expected a non-null collation");
	}
	auto splits = StringUtil::Split(val.str_value, "_");
	if (splits.size() == 1) {
		return make_unique<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_unique<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InternalException("Expected one or two splits");
	}
}

static ScalarFunction GetICUFunction(string collation) {
	return ScalarFunction(collation, {LogicalType::VARCHAR}, LogicalType::VARCHAR, ICUCollateFunction, false,
	                      ICUCollateBind);
}

static void SetICUTimeZone(ClientContext &context, SetScope scope, Value &parameter) {
	icu::StringPiece utf8(parameter.Value::GetValueUnsafe<string>());
	const auto uid = icu::UnicodeString::fromUTF8(utf8);
	std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(uid));
	if (*tz == icu::TimeZone::getUnknown()) {
		throw NotImplementedException("Unknown TimeZone setting");
	}
}

struct ICUTimeZoneData : public FunctionOperatorData {
	ICUTimeZoneData() : tzs(icu::TimeZone::createEnumeration()) {
		UErrorCode status = U_ZERO_ERROR;
		std::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(status));
		now = calendar->getNow();
	}

	std::unique_ptr<icu::StringEnumeration> tzs;
	UDate now;
};

static unique_ptr<FunctionData> ICUTimeZoneBind(ClientContext &context, vector<Value> &inputs,
                                                unordered_map<string, Value> &named_parameters,
                                                vector<LogicalType> &input_table_types,
                                                vector<string> &input_table_names, vector<LogicalType> &return_types,
                                                vector<string> &names) {
	names.emplace_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("abbrev");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("utc_offset");
	return_types.push_back(LogicalType::INTERVAL);
	names.emplace_back("is_dst");
	return_types.push_back(LogicalType::BOOLEAN);

	return nullptr;
}

static unique_ptr<FunctionOperatorData> ICUTimeZoneInit(ClientContext &context, const FunctionData *bind_data,
                                                        const vector<column_t> &column_ids,
                                                        TableFilterCollection *filters) {
	return make_unique<ICUTimeZoneData>();
}

static void ICUTimeZoneCleanup(ClientContext &context, const FunctionData *bind_data,
                               FunctionOperatorData *operator_state) {
	auto &data = (ICUTimeZoneData &)*operator_state;
	data.tzs.release();
}

static void ICUTimeZoneFunction(ClientContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (ICUTimeZoneData &)*operator_state;
	idx_t index = 0;
	while (index < STANDARD_VECTOR_SIZE) {
		UErrorCode status = U_ZERO_ERROR;
		auto long_id = data.tzs->snext(status);
		if (U_FAILURE(status) || !long_id) {
			break;
		}

		//	The LONG name is the one we looked up
		std::string utf8;
		long_id->toUTF8String(utf8);
		output.SetValue(0, index, Value(utf8));

		//	We don't have the zone tree for determining abbreviated names,
		//	so the SHORT name is the first equivalent TZ without a slash.
		icu::UnicodeString short_id = *long_id;
		const auto nIDs = icu::TimeZone::countEquivalentIDs(*long_id);
		for (int32_t idx = 0; idx < nIDs; ++idx) {
			const auto eid = icu::TimeZone::getEquivalentID(*long_id, idx);
			if (eid.indexOf(char16_t('/')) < 0) {
				short_id = eid;
				break;
			}
		}

		utf8.clear();
		short_id.toUTF8String(utf8);
		output.SetValue(1, index, Value(utf8));

		std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(*long_id));
		int32_t rawOffsetMS;
		int32_t dstOffsetMS;
		tz->getOffset(data.now, false, rawOffsetMS, dstOffsetMS, status);
		if (U_FAILURE(status)) {
			break;
		}

		output.SetValue(2, index, Value::INTERVAL(Interval::FromMicro(rawOffsetMS * Interval::MICROS_PER_MSEC)));
		output.SetValue(3, index, Value(dstOffsetMS != 0));
		++index;
	}
	output.SetCardinality(index);
}

struct ICUDatePart {
	using CalendarPtr = unique_ptr<icu::Calendar>;
	typedef int32_t (*PartAdapter)(icu::Calendar *calendar, const uint64_t micros);

	static DatePartSpecifier PartCodeFromFunction(const string &name) {
		return GetDatePartSpecifier(name.substr(4));
	}

	static int32_t ExtractField(icu::Calendar *calendar, UCalendarDateFields field) {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->get(field, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to extract ICU date part.");
		}
		return result;
	}

	// Date part adapters
	static int32_t ExtractYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_YEAR);
	}

	static int32_t ExtractDecade(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) / 10;
	}

	static int32_t ExtractCentury(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 100;
	}

	static int32_t ExtractMillenium(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 1000;
	}

	static int32_t ExtractMonth(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) + 1;
	}

	static int32_t ExtractQuarter(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) / Interval::MONTHS_PER_QUARTER + 1;
	}

	static int32_t ExtractDay(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DATE);
	}

	static int32_t ExtractDayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK) - UCAL_SUNDAY;
	}

	static int32_t ExtractISODayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK);
	}

	static int32_t ExtractWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		return ExtractField(calendar, UCAL_WEEK_OF_YEAR);
	}

	static int32_t ExtractYearWeek(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) * 100 + ExtractWeek(calendar, micros);
	}

	static int32_t ExtractDayOfYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DAY_OF_YEAR);
	}

	static int32_t ExtractHour(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_HOUR_OF_DAY);
	}

	static int32_t ExtractMinute(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MINUTE);
	}

	static int32_t ExtractSecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_SECOND);
	}

	static int32_t ExtractMillisecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractSecond(calendar, micros) * Interval::MSECS_PER_SEC + ExtractField(calendar, UCAL_MILLISECOND);
	}

	static int32_t ExtractMicrosecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractMillisecond(calendar, micros) * Interval::MICROS_PER_MSEC + micros;
	}

	static int32_t ExtractEpoch(icu::Calendar *calendar, const uint64_t micros) {
		UErrorCode status = U_ZERO_ERROR;
		auto millis = calendar->getTime(status);
		millis -= ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis -= ExtractField(calendar, UCAL_DST_OFFSET);
		//	Truncate
		return int32_t(millis / Interval::MSECS_PER_SEC);
	}

	static PartAdapter PartCodeAdapterFactory(DatePartSpecifier part) {
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
		case DatePartSpecifier::DOY:
			return ExtractDayOfYear;
		case DatePartSpecifier::QUARTER:
			return ExtractQuarter;
		case DatePartSpecifier::YEARWEEK:
			return ExtractYearWeek;
		case DatePartSpecifier::EPOCH:
			return ExtractEpoch;
		default:
			throw Exception("Unsupported ICU extract adapter");
		}
	}

	struct BindData : public FunctionData {
		BindData(CalendarPtr calendar_p, PartAdapter adapter_p) : calendar(move(calendar_p)), adapter(adapter_p) {
		}

		CalendarPtr calendar;
		PartAdapter adapter;

		unique_ptr<FunctionData> Copy() override {
			return make_unique<BindData>(CalendarPtr(calendar->clone()), adapter);
		}
	};

	static void UnaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<timestamp_t, int32_t>(args.data[0], result, args.size(), [&](timestamp_t input) {
			UErrorCode status = U_ZERO_ERROR;

			const UDate millis = input.value / Interval::MICROS_PER_MSEC;
			const auto micros = input.value % Interval::MICROS_PER_MSEC;
			calendar->setTime(millis, status);
			if (U_FAILURE(status)) {
				throw Exception("Unable to compute ICU date part.");
			}
			return info.adapter(calendar.get(), micros);
		});
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		auto part = PartCodeFromFunction(bound_function.name);
		auto adapter = PartCodeAdapterFactory(part);

		Value tz_value;
		string tz_id;
		if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
			tz_id = tz_value.ToString();
		}
		auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

		UErrorCode success = U_ZERO_ERROR;
		CalendarPtr calendar(icu::Calendar::createInstance(tz, success));
		if (U_FAILURE(success)) {
			throw Exception("Unable to create ICU date part calendar.");
		}

		return make_unique<BindData>(move(calendar), adapter);
	}

	static ScalarFunction GetUnaryTimestampFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::TIMESTAMP}, LogicalType::INTEGER, UnaryFunction, false, Bind);
	}

	static void AddUnaryTimestampFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetUnaryTimestampFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.CreateFunction(context, &func_info);
	}
};

void ICUExtension::Load(DuckDB &db) {
	// load the collations
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	// iterate over all the collations
	int32_t count;
	auto locales = icu::Collator::getAvailableLocales(count);
	for (int32_t i = 0; i < count; i++) {
		string collation;
		if (string(locales[i].getCountry()).empty()) {
			// language only
			collation = locales[i].getLanguage();
		} else {
			// language + country
			collation = locales[i].getLanguage() + string("_") + locales[i].getCountry();
		}
		collation = StringUtil::Lower(collation);

		CreateCollationInfo info(collation, GetICUFunction(collation), false, true);
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		catalog.CreateCollation(*con.context, &info);
	}
	ScalarFunction sort_key("icu_sort_key", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        ICUCollateFunction, false, ICUSortKeyBind);

	CreateScalarFunctionInfo sort_key_info(move(sort_key));
	catalog.CreateFunction(*con.context, &sort_key_info);

	// Time Zones
	auto &config = DBConfig::GetConfig(*db.instance);
	config.AddExtensionOption("TimeZone", "The current time zone", LogicalType::VARCHAR, SetICUTimeZone);
	Value utc("UTC");
	config.set_variables["TimeZone"] = move(utc);

	TableFunction tz_names("pg_timezone_names", {}, ICUTimeZoneFunction, ICUTimeZoneBind, ICUTimeZoneInit, nullptr,
	                       ICUTimeZoneCleanup);
	CreateTableFunctionInfo tz_names_info(move(tz_names));
	catalog.CreateTableFunction(*con.context, &tz_names_info);

	// Part functions
	ICUDatePart::AddUnaryTimestampFunction("icu_year", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_month", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_day", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_decade", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_century", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_millennium", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_microsecond", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_millisecond", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_second", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_minute", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_hour", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_dayofweek", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_isodow", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_week", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_dayofyear", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_quarter", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_yearweek", *con.context);
	ICUDatePart::AddUnaryTimestampFunction("icu_epoch", *con.context);

	con.Commit();
}

} // namespace duckdb
