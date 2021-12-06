#include "include/icu-datefunc.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<FunctionData> ICUDateFunc::Bind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	Value tz_value;
	string tz_id;
	if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
		tz_id = tz_value.ToString();
	}
	auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

	UErrorCode success = U_ZERO_ERROR;
	CalendarPtr calendar(icu::Calendar::createInstance(tz, success));
	if (U_FAILURE(success)) {
		throw Exception("Unable to create ICU calendar.");
	}

	return make_unique<BindData>(move(calendar));
}

} // namespace duckdb
