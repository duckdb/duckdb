#include "include/icu-datefunc.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

ICUDateFunc::BindData::BindData(const BindData &other) : calendar(other.calendar->clone()) {
}

ICUDateFunc::BindData::BindData(ClientContext &context) {
	Value tz_value;
	string tz_id;
	if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
		tz_id = tz_value.ToString();
	}
	auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

	UErrorCode success = U_ZERO_ERROR;
	calendar.reset(icu::Calendar::createInstance(tz, success));
	if (U_FAILURE(success)) {
		throw Exception("Unable to create ICU calendar.");
	}
}

unique_ptr<FunctionData> ICUDateFunc::BindData::Copy() {
	return make_unique<BindData>(*this);
}

unique_ptr<FunctionData> ICUDateFunc::Bind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	return make_unique<BindData>(context);
}

} // namespace duckdb
