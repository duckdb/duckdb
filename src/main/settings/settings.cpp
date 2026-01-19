#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

bool Settings::TryGetSettingInternal(const ClientContext &context, const char *setting, Value &result) {
	return context.TryGetCurrentUserSetting(setting, result);
}

bool Settings::TryGetSettingInternal(const DBConfig &config, const char *setting, Value &result) {
	auto lookup_result = config.TryGetCurrentUserSetting(setting, result);
	return lookup_result;
}

bool Settings::TryGetSettingInternal(const DatabaseInstance &db, const char *setting, Value &result) {
	return TryGetSettingInternal(DBConfig::GetConfig(db), setting, result);
}

} // namespace duckdb
