#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

bool Settings::TryGetSettingInternal(const ClientContext &context, idx_t setting_index, Value &result) {
	return context.TryGetCurrentUserSetting(setting_index, result);
}

bool Settings::TryGetSettingInternal(const DBConfig &config, idx_t setting_index, Value &result) {
	auto lookup_result = config.TryGetCurrentUserSetting(setting_index, result);
	return lookup_result;
}

bool Settings::TryGetSettingInternal(const DatabaseInstance &db, idx_t setting_index, Value &result) {
	return TryGetSettingInternal(DBConfig::GetConfig(db), setting_index, result);
}

Value Settings::FormatDisplayValue(ClientContext &context, const Value &value) {
	if (!value.IsNull() && value.type().id() == LogicalTypeId::BOOLEAN) {
		return Value(BooleanValue::Get(value) ? "on" : "off");
	}
	return value.CastAs(context, LogicalType::VARCHAR);
}

} // namespace duckdb
