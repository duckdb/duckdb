#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "include/icu-current.hpp"
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
#include "unicode/stringpiece.h"
#include "unicode/timezone.h"
#include "unicode/ucol.h"
#include "icu-helpers.hpp"

#include <cassert>

namespace duckdb {

struct IcuBindData : public FunctionData {
	duckdb::unique_ptr<icu::Collator> collator;
	string language;
	string country;
	string tag;

	explicit IcuBindData(duckdb::unique_ptr<icu::Collator> collator_p) : collator(std::move(collator_p)) {
	}

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

	explicit IcuBindData(string tag_p) : tag(std::move(tag_p)) {
		UErrorCode status = U_ZERO_ERROR;
		UCollator *ucollator = ucol_open(tag.c_str(), &status);
		if (U_FAILURE(status)) {
			auto error_name = u_errorName(status);
			throw InvalidInputException("Failed to create ICU collator with tag %s: %s", tag, error_name);
		}
		collator = unique_ptr<icu::Collator>(icu::Collator::fromUCollator(ucollator));
	}

	static duckdb::unique_ptr<FunctionData> CreateInstance(string language, string country, string tag) {
		//! give priority to tagged collation
		if (!tag.empty()) {
			return make_uniq<IcuBindData>(tag);
		} else {
			return make_uniq<IcuBindData>(language, country);
		}
	}

	duckdb::unique_ptr<FunctionData> Copy() const override {
		return CreateInstance(language, country, tag);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IcuBindData>();
		return language == other.language && country == other.country && tag == other.tag;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<IcuBindData>();
		serializer.WriteProperty(100, "language", bind_data.language);
		serializer.WriteProperty(101, "country", bind_data.country);
		serializer.WritePropertyWithDefault<string>(102, "tag", bind_data.tag);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &function) {
		string language;
		string country;
		string tag;
		deserializer.ReadProperty(100, "language", language);
		deserializer.ReadProperty(101, "country", country);
		deserializer.ReadPropertyWithDefault<string>(102, "tag", tag);
		return CreateInstance(language, country, tag);
	}

	static const string FUNCTION_PREFIX;

	static string EncodeFunctionName(const string &collation) {
		return FUNCTION_PREFIX + collation;
	}
	static string DecodeFunctionName(const string &fname) {
		return fname.substr(FUNCTION_PREFIX.size());
	}
};

const string IcuBindData::FUNCTION_PREFIX = "icu_collate_";

static int32_t ICUGetSortKey(icu::Collator &collator, string_t input, duckdb::unique_ptr<char[]> &buffer,
                             int32_t &buffer_size) {
	icu::UnicodeString unicode_string =
	    icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetData(), int32_t(input.GetSize())));
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
		return str_result;
	});
}

static duckdb::unique_ptr<FunctionData> ICUCollateBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<duckdb::unique_ptr<Expression>> &arguments) {
	//! Return a tagged collator
	if (!bound_function.extra_info.empty()) {
		return make_uniq<IcuBindData>(bound_function.extra_info);
	}

	const auto collation = IcuBindData::DecodeFunctionName(bound_function.name);
	auto splits = StringUtil::Split(collation, "_");
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
	//! Verify tagged collation
	if (!bound_function.extra_info.empty()) {
		return make_uniq<IcuBindData>(bound_function.extra_info);
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

static ScalarFunction GetICUCollateFunction(const string &collation, const string &tag) {
	string fname = IcuBindData::EncodeFunctionName(collation);
	ScalarFunction result(fname, {LogicalType::VARCHAR}, LogicalType::VARCHAR, ICUCollateFunction, ICUCollateBind);
	//! collation tag is added into the Function extra info
	result.extra_info = tag;
	result.serialize = IcuBindData::Serialize;
	result.deserialize = IcuBindData::Deserialize;
	return result;
}

unique_ptr<icu::TimeZone> GetKnownTimeZone(const string &tz_str) {
	icu::StringPiece tz_name_utf8(tz_str);
	const auto uid = icu::UnicodeString::fromUTF8(tz_name_utf8);
	duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(uid));
	if (*tz != icu::TimeZone::getUnknown()) {
		return tz;
	}

	return nullptr;
}

static string NormalizeTimeZone(const string &tz_str) {
	if (GetKnownTimeZone(tz_str)) {
		return tz_str;
	}

	//	Map UTC±NN00 to Etc/UTC±N
	do {
		if (tz_str.size() <= 4) {
			break;
		}
		if (tz_str.compare(0, 3, "UTC")) {
			break;
		}

		idx_t pos = 3;
		const auto sign = tz_str[pos++];
		if (sign != '+' && sign != '-') {
			break;
		}

		string mapped = "Etc/GMT";
		mapped += sign;
		const auto base_len = mapped.size();
		for (; pos < tz_str.size(); ++pos) {
			const auto digit = tz_str[pos];
			//	We could get fancy here and count colons and their locations, but I doubt anyone cares.
			if (digit == '0' || digit == ':') {
				continue;
			}
			if (!StringUtil::CharacterIsDigit(digit)) {
				break;
			}
			mapped += digit;
		}
		if (pos < tz_str.size()) {
			break;
		}
		// If we didn't add anything, then make it +0
		if (mapped.size() == base_len) {
			mapped.back() = '+';
			mapped += '0';
		}
		// Final sanity check
		if (GetKnownTimeZone(mapped)) {
			return mapped;
		}
	} while (false);

	return tz_str;
}

unique_ptr<icu::TimeZone> GetTimeZoneInternal(string &tz_str, vector<string> &candidates) {
	auto tz = GetKnownTimeZone(tz_str);
	if (tz) {
		return tz;
	}

	// Try to be friendlier
	// Go through all the zone names and look for a case insensitive match
	// If we don't find one, make a suggestion
	// FIXME: this is very inefficient
	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(status));
	duckdb::unique_ptr<icu::StringEnumeration> tzs(icu::TimeZone::createEnumeration());
	for (;;) {
		auto long_id = tzs->snext(status);
		if (U_FAILURE(status) || !long_id) {
			break;
		}
		std::string candidate_tz_name;
		long_id->toUTF8String(candidate_tz_name);
		if (StringUtil::CIEquals(candidate_tz_name, tz_str)) {
			// case insensitive match - return this timezone instead
			tz_str = candidate_tz_name;
			icu::StringPiece utf8(tz_str);
			const auto tz_unicode_str = icu::UnicodeString::fromUTF8(utf8);
			duckdb::unique_ptr<icu::TimeZone> insensitive_tz(icu::TimeZone::createTimeZone(tz_unicode_str));
			return insensitive_tz;
		}

		candidates.emplace_back(candidate_tz_name);
	}
	return nullptr;
}

unique_ptr<icu::TimeZone> ICUHelpers::TryGetTimeZone(string &tz_str) {
	vector<string> candidates;
	return GetTimeZoneInternal(tz_str, candidates);
}

unique_ptr<icu::TimeZone> ICUHelpers::GetTimeZone(string &tz_str, string *error_message) {
	vector<string> candidates;
	auto tz = GetTimeZoneInternal(tz_str, candidates);
	if (tz) {
		return tz;
	}
	string candidate_str =
	    StringUtil::CandidatesMessage(StringUtil::TopNJaroWinkler(candidates, tz_str), "Candidate time zones");
	if (error_message) {
		duckdb::stringstream ss;
		ss << "Unknown TimeZone '" << tz_str << "'!\n" << candidate_str;
		*error_message = ss.str();
		return nullptr;
	}
	throw NotImplementedException("Unknown TimeZone '%s'!\n%s", tz_str, candidate_str);
}

static void SetICUTimeZone(ClientContext &context, SetScope scope, Value &parameter) {
	auto tz_str = StringValue::Get(parameter);
	tz_str = NormalizeTimeZone(tz_str);
	ICUHelpers::GetTimeZone(tz_str);
	parameter = Value(tz_str);
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
	if (!U_FAILURE(status) && name == cal->getType()) {
		return;
	}

	//	Try to be friendlier
	//	Go through all the calendar names and look for a case insensitive match
	//	If we don't find one, make a suggestion
	status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::StringEnumeration> calendars;
	calendars.reset(icu::Calendar::getKeywordValuesForLocale("calendar", icu::Locale::getDefault(), false, status));

	vector<string> candidates;
	for (;;) {
		auto calendar = calendars->snext(status);
		if (U_FAILURE(status) || !calendar) {
			break;
		}

		std::string utf8;
		calendar->toUTF8String(utf8);
		if (StringUtil::CIEquals(utf8, name)) {
			parameter = Value(utf8);
			return;
		}

		candidates.emplace_back(utf8);
	}

	string candidate_str =
	    StringUtil::CandidatesMessage(StringUtil::TopNJaroWinkler(candidates, name), "Candidate calendars");

	throw NotImplementedException("Unknown Calendar '%s'!\n%s", name, candidate_str);
}

static void LoadInternal(ExtensionLoader &loader) {
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

		CreateCollationInfo info(collation, GetICUCollateFunction(collation, ""), false, false);
		loader.RegisterCollation(info);
	}

	/**
	 * This collation function is inpired on the Postgres "ignore_accents":
	 * See: https://www.postgresql.org/docs/current/collation.html
	 * CREATE COLLATION ignore_accents (provider = icu, locale = 'und-u-ks-level1-kc-true', deterministic = false);
	 *
	 * Also, according with the source file: postgres/src/backend/utils/adt/pg_locale.c.
	 * "und-u-kc-ks-level1" is converted to the equivalent ICU format locale ID,
	 * e.g. "und@colcaselevel=yes;colstrength=primary"
	 *
	 */
	CreateCollationInfo info("icu_noaccent", GetICUCollateFunction("noaccent", "und-u-ks-level1-kc-true"), false,
	                         false);
	loader.RegisterCollation(info);

	ScalarFunction sort_key("icu_sort_key", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        ICUCollateFunction, ICUSortKeyBind);
	loader.RegisterFunction(sort_key);

	// Time Zones
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createDefault());
	icu::UnicodeString tz_id;
	std::string tz_string;
	tz->getID(tz_id).toUTF8String(tz_string);
	// If the environment TZ is invalid, look for some alternatives
	tz_string = NormalizeTimeZone(tz_string);
	if (!GetKnownTimeZone(tz_string)) {
		tz_string = "UTC";
	}
	config.AddExtensionOption("TimeZone", "The current time zone", LogicalType::VARCHAR, Value(tz_string),
	                          SetICUTimeZone);

	RegisterICUCurrentFunctions(loader);
	RegisterICUDateAddFunctions(loader);
	RegisterICUDatePartFunctions(loader);
	RegisterICUDateSubFunctions(loader);
	RegisterICUDateTruncFunctions(loader);
	RegisterICUMakeDateFunctions(loader);
	RegisterICUTableRangeFunctions(loader);
	RegisterICUListRangeFunctions(loader);
	RegisterICUStrptimeFunctions(loader);
	RegisterICUTimeBucketFunctions(loader);
	RegisterICUTimeZoneFunctions(loader);

	// Calendars
	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::Calendar> cal(icu::Calendar::createInstance(status));
	config.AddExtensionOption("Calendar", "The current calendar", LogicalType::VARCHAR, Value(cal->getType()),
	                          SetICUCalendar);

	TableFunction cal_names("icu_calendar_names", {}, ICUCalendarFunction, ICUCalendarBind, ICUCalendarInit);
	loader.RegisterFunction(cal_names);
}

void IcuExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string IcuExtension::Name() {
	return "icu";
}

std::string IcuExtension::Version() const {
#ifdef EXT_VERSION_ICU
	return EXT_VERSION_ICU;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(icu, loader) { // NOLINT
	duckdb::LoadInternal(loader);
}
}
