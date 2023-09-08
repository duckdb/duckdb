#define DUCKDB_EXTENSION_MAIN

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "include/icu-dateadd.hpp"
#include "include/icu-datepart.hpp"
#include "include/icu-datesub.hpp"
#include "include/icu-datetrunc.hpp"
#include "include/icu-list-range.hpp"
#include "include/icu-makedate.hpp"
#include "include/icu-strptime.hpp"
#include "include/icu-table-range.hpp"
#include "include/icu-timebucket.hpp"
#include "include/icu-timezone.hpp"
#include "include/icu_extension.hpp"
#include "unicode/calendar.h"
#include "unicode/coll.h"
#include "unicode/sortkey.h"
#include "unicode/stringpiece.h"
#include "unicode/timezone.h"
#include "unicode/ucol.h"

#include <cassert>

namespace duckdb {

struct IcuBindData : public FunctionData {
	duckdb::unique_ptr<icu::Collator> collator;
	string language;
	string country;

	IcuBindData(string language_p, string country_p) : language(std::move(language_p)), country(std::move(country_p)) {
		UErrorCode status = U_ZERO_ERROR;
		auto locale = icu::Locale(language.c_str(), country.c_str());
		if (locale.isBogus()) {
			throw InvalidInputException("Locale is bogus!?");
		}
		this->collator = duckdb::unique_ptr<icu::Collator>(icu::Collator::createInstance(locale, status));
		if (U_FAILURE(status)) {
			auto error_name = u_errorName(status);
			throw InvalidInputException("Failed to create ICU collator: %s (language: %s, country: %s)", error_name,
			                            language, country);
		}
	}

	duckdb::unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IcuBindData>(language, country);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IcuBindData>();
		return language == other.language && country == other.country;
	}
};

static int32_t ICUGetSortKey(icu::Collator &collator, string_t input, duckdb::unique_ptr<char[]> &buffer,
                             int32_t &buffer_size) {
	icu::UnicodeString unicode_string =
	    icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetData(), input.GetSize()));
	int32_t string_size = collator.getSortKey(unicode_string, reinterpret_cast<uint8_t *>(buffer.get()), buffer_size);
	if (string_size > buffer_size) {
		// have to resize the buffer
		buffer_size = string_size;
		buffer = duckdb::unique_ptr<char[]>(new char[buffer_size]);

		string_size = collator.getSortKey(unicode_string, reinterpret_cast<uint8_t *>(buffer.get()), buffer_size);
	}
	return string_size;
}

static void ICUCollateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const char HEX_TABLE[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<IcuBindData>();
	auto &collator = *info.collator;

	duckdb::unique_ptr<char[]> buffer;
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
		str_result.Finalize();
		// printf("%s: %s\n", input.GetString().c_str(), str_result.GetString().c_str());
		return str_result;
	});
}

static duckdb::unique_ptr<FunctionData> ICUCollateBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<duckdb::unique_ptr<Expression>> &arguments) {
	auto splits = StringUtil::Split(bound_function.name, "_");
	if (splits.size() == 1) {
		return make_uniq<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_uniq<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InvalidInputException("Expected one or two splits");
	}
}

static duckdb::unique_ptr<FunctionData> ICUSortKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<duckdb::unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsFoldable()) {
		throw NotImplementedException("ICU_SORT_KEY(VARCHAR, VARCHAR) with non-constant collation is not supported");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]).CastAs(context, LogicalType::VARCHAR);
	if (val.IsNull()) {
		throw NotImplementedException("ICU_SORT_KEY(VARCHAR, VARCHAR) expected a non-null collation");
	}
	auto splits = StringUtil::Split(StringValue::Get(val), "_");
	if (splits.size() == 1) {
		return make_uniq<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_uniq<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InvalidInputException("Expected one or two splits");
	}
}

static void ICUCollateSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                const ScalarFunction &function) {
	throw NotImplementedException("FIXME: serialize icu-collate");
}

static duckdb::unique_ptr<FunctionData> ICUCollateDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	throw NotImplementedException("FIXME: serialize icu-collate");
}

static ScalarFunction GetICUFunction(const string &collation) {
	ScalarFunction result(collation, {LogicalType::VARCHAR}, LogicalType::VARCHAR, ICUCollateFunction, ICUCollateBind);
	result.serialize = ICUCollateSerialize;
	result.deserialize = ICUCollateDeserialize;
	return result;
}

static void SetICUTimeZone(ClientContext &context, SetScope scope, Value &parameter) {
	icu::StringPiece utf8(StringValue::Get(parameter));
	const auto uid = icu::UnicodeString::fromUTF8(utf8);
	duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(uid));
	if (*tz == icu::TimeZone::getUnknown()) {
		throw NotImplementedException("Unknown TimeZone setting");
	}
}

struct ICUCalendarData : public GlobalTableFunctionState {
	ICUCalendarData() {
		// All calendars are available in all locales
		UErrorCode status = U_ZERO_ERROR;
		calendars.reset(icu::Calendar::getKeywordValuesForLocale("calendar", icu::Locale::getDefault(), false, status));
	}

	duckdb::unique_ptr<icu::StringEnumeration> calendars;
};

static duckdb::unique_ptr<FunctionData> ICUCalendarBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static duckdb::unique_ptr<GlobalTableFunctionState> ICUCalendarInit(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<ICUCalendarData>();
}

static void ICUCalendarFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<ICUCalendarData>();
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
	duckdb::unique_ptr<icu::Calendar> cal(icu::Calendar::createInstance(locale, status));
	if (U_FAILURE(status) || name != cal->getType()) {
		throw NotImplementedException("Unknown Calendar setting");
	}
}

void IcuExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetSystemCatalog(*con.context);

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
		catalog.CreateCollation(*con.context, info);
	}
	ScalarFunction sort_key("icu_sort_key", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        ICUCollateFunction, ICUSortKeyBind);

	CreateScalarFunctionInfo sort_key_info(std::move(sort_key));
	catalog.CreateFunction(*con.context, sort_key_info);

	// Time Zones
	auto &config = DBConfig::GetConfig(*db.instance);
	duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createDefault());
	icu::UnicodeString tz_id;
	std::string tz_string;
	tz->getID(tz_id).toUTF8String(tz_string);
	config.AddExtensionOption("TimeZone", "The current time zone", LogicalType::VARCHAR, Value(tz_string),
	                          SetICUTimeZone);

	RegisterICUDateAddFunctions(*con.context);
	RegisterICUDatePartFunctions(*con.context);
	RegisterICUDateSubFunctions(*con.context);
	RegisterICUDateTruncFunctions(*con.context);
	RegisterICUMakeDateFunctions(*con.context);
	RegisterICUTableRangeFunctions(*con.context);
	RegisterICUListRangeFunctions(*con.context);
	RegisterICUStrptimeFunctions(*con.context);
	RegisterICUTimeBucketFunctions(*con.context);
	RegisterICUTimeZoneFunctions(*con.context);

	// Calendars
	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::Calendar> cal(icu::Calendar::createInstance(status));
	config.AddExtensionOption("Calendar", "The current calendar", LogicalType::VARCHAR, Value(cal->getType()),
	                          SetICUCalendar);

	TableFunction cal_names("icu_calendar_names", {}, ICUCalendarFunction, ICUCalendarBind, ICUCalendarInit);
	CreateTableFunctionInfo cal_names_info(std::move(cal_names));
	catalog.CreateTableFunction(*con.context, cal_names_info);

	con.Commit();
}

std::string IcuExtension::Name() {
	return "icu";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void icu_init(duckdb::DatabaseInstance &db) { // NOLINT
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::IcuExtension>();
}

DUCKDB_EXTENSION_API const char *icu_version() { // NOLINT
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
