#define DUCKDB_EXTENSION_MAIN

#include "unicode/ucol.h"
#include "unicode/stringpiece.h"
#include "unicode/coll.h"
#include "unicode/sortkey.h"
#include "unicode/timezone.h"
#include "unicode/calendar.h"

#include "include/icu-extension.hpp"
#include "include/icu-dateadd.hpp"
#include "include/icu-datepart.hpp"
#include "include/icu-datesub.hpp"
#include "include/icu-datetrunc.hpp"
#include "include/icu-makedate.hpp"
#include "include/icu-strptime.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"

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
		auto locale = icu::Locale(language.c_str(), country.c_str());
		if (locale.isBogus()) {
			throw InternalException("Locale is bogus!?");
		}
		this->collator = std::unique_ptr<icu::Collator>(icu::Collator::createInstance(locale, status));
		if (U_FAILURE(status)) {
			auto error_name = u_errorName(status);
			throw InternalException("Failed to create ICU collator: %s (language: %s, country: %s)", error_name,
			                        language, country);
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<IcuBindData>(language, country);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (IcuBindData &)other_p;
		return language == other.language && country == other.country;
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
		const auto string_size = idx_t(ICUGetSortKey(collator, input, buffer, buffer_size));
		// convert the sort key to hexadecimal
		auto str_result = StringVector::EmptyString(result, (string_size - 1) * 2);
		auto str_data = str_result.GetDataWriteable();
		for (idx_t i = 0; i < string_size - 1; i++) {
			uint8_t byte = uint8_t(buffer[i]);
			D_ASSERT(byte != 0);
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
	if (val.IsNull()) {
		throw NotImplementedException("ICU_SORT_KEY(VARCHAR, VARCHAR) expected a non-null collation");
	}
	auto splits = StringUtil::Split(StringValue::Get(val), "_");
	if (splits.size() == 1) {
		return make_unique<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_unique<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InternalException("Expected one or two splits");
	}
}

static ScalarFunction GetICUFunction(const string &collation) {
	return ScalarFunction(collation, {LogicalType::VARCHAR}, LogicalType::VARCHAR, ICUCollateFunction, false,
	                      ICUCollateBind);
}

static void SetICUTimeZone(ClientContext &context, SetScope scope, Value &parameter) {
	icu::StringPiece utf8(StringValue::Get(parameter));
	const auto uid = icu::UnicodeString::fromUTF8(utf8);
	std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(uid));
	if (*tz == icu::TimeZone::getUnknown()) {
		throw NotImplementedException("Unknown TimeZone setting");
	}
}

struct ICUTimeZoneData : public GlobalTableFunctionState {
	ICUTimeZoneData() : tzs(icu::TimeZone::createEnumeration()) {
		UErrorCode status = U_ZERO_ERROR;
		std::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(status));
		now = calendar->getNow();
	}

	std::unique_ptr<icu::StringEnumeration> tzs;
	UDate now;
};

static unique_ptr<FunctionData> ICUTimeZoneBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("abbrev");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("utc_offset");
	return_types.emplace_back(LogicalType::INTERVAL);
	names.emplace_back("is_dst");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> ICUTimeZoneInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<ICUTimeZoneData>();
}

static void ICUTimeZoneFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ICUTimeZoneData &)*data_p.global_state;
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
		int32_t raw_offset_ms;
		int32_t dst_offset_ms;
		tz->getOffset(data.now, false, raw_offset_ms, dst_offset_ms, status);
		if (U_FAILURE(status)) {
			break;
		}

		output.SetValue(2, index, Value::INTERVAL(Interval::FromMicro(raw_offset_ms * Interval::MICROS_PER_MSEC)));
		output.SetValue(3, index, Value(dst_offset_ms != 0));
		++index;
	}
	output.SetCardinality(index);
}

struct ICUCalendarData : public GlobalTableFunctionState {
	ICUCalendarData() {
		// All calendars are available in all locales
		UErrorCode status = U_ZERO_ERROR;
		calendars.reset(icu::Calendar::getKeywordValuesForLocale("calendar", icu::Locale::getDefault(), false, status));
	}

	std::unique_ptr<icu::StringEnumeration> calendars;
};

static unique_ptr<FunctionData> ICUCalendarBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> ICUCalendarInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<ICUCalendarData>();
}

static void ICUCalendarFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ICUCalendarData &)*data_p.global_state;
	idx_t index = 0;
	while (index < STANDARD_VECTOR_SIZE) {
		if (!data.calendars) {
			break;
		}

		UErrorCode status = U_ZERO_ERROR;
		auto calendar = data.calendars->snext(status);
		if (U_FAILURE(status) || !calendar) {
			break;
		}

		//	The calendar name is all we have
		std::string utf8;
		calendar->toUTF8String(utf8);
		output.SetValue(0, index, Value(utf8));

		++index;
	}
	output.SetCardinality(index);
}

static void SetICUCalendar(ClientContext &context, SetScope scope, Value &parameter) {
	const auto name = parameter.Value::GetValueUnsafe<string>();
	string locale_key = "@calendar=" + name;
	icu::Locale locale(locale_key.c_str());

	UErrorCode status = U_ZERO_ERROR;
	std::unique_ptr<icu::Calendar> cal(icu::Calendar::createInstance(locale, status));
	if (U_FAILURE(status) || name != cal->getType()) {
		throw NotImplementedException("Unknown Calendar setting");
	}
}

void ICUExtension::Load(DuckDB &db) {
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
	std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createDefault());
	icu::UnicodeString tz_id;
	std::string tz_string;
	tz->getID(tz_id).toUTF8String(tz_string);
	config.set_variables["TimeZone"] = Value(tz_string);

	TableFunction tz_names("pg_timezone_names", {}, ICUTimeZoneFunction, ICUTimeZoneBind, ICUTimeZoneInit);
	CreateTableFunctionInfo tz_names_info(move(tz_names));
	catalog.CreateTableFunction(*con.context, &tz_names_info);

	RegisterICUDateAddFunctions(*con.context);
	RegisterICUDatePartFunctions(*con.context);
	RegisterICUDateSubFunctions(*con.context);
	RegisterICUDateTruncFunctions(*con.context);
	RegisterICUMakeDateFunctions(*con.context);
	RegisterICUStrptimeFunctions(*con.context);

	// Calendars
	config.AddExtensionOption("Calendar", "The current calendar", LogicalType::VARCHAR, SetICUCalendar);
	UErrorCode status = U_ZERO_ERROR;
	std::unique_ptr<icu::Calendar> cal(icu::Calendar::createInstance(status));
	config.set_variables["Calendar"] = Value(cal->getType());

	TableFunction cal_names("icu_calendar_names", {}, ICUCalendarFunction, ICUCalendarBind, ICUCalendarInit);
	CreateTableFunctionInfo cal_names_info(move(cal_names));
	catalog.CreateTableFunction(*con.context, &cal_names_info);

	con.Commit();
}

std::string ICUExtension::Name() {
	return "icu";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void icu_init(duckdb::DatabaseInstance &db) { // NOLINT
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ICUExtension>();
}

DUCKDB_EXTENSION_API const char *icu_version() { // NOLINT
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
