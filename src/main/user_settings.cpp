#include "duckdb/main/user_settings.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/types/string.hpp"

namespace duckdb {

void UserSettingsMap::SetUserSetting(idx_t setting_index, Value target_value) {
	if (setting_index >= settings.size()) {
		settings.resize(setting_index + 1);
	}
	auto &generic_setting = settings[setting_index];
	generic_setting.is_set = true;
	generic_setting.value = std::move(target_value);
}

void UserSettingsMap::ClearSetting(idx_t setting_index) {
	if (setting_index >= settings.size()) {
		// never set
		return;
	}
	auto &generic_setting = settings[setting_index];
	generic_setting.is_set = false;
	generic_setting.value = Value();
}

bool UserSettingsMap::IsSet(idx_t setting_index) const {
	if (setting_index >= settings.size()) {
		// never set
		return false;
	}
	return settings[setting_index].is_set;
}

bool UserSettingsMap::TryGetSetting(idx_t setting_index, Value &result_value) const {
	if (setting_index >= settings.size()) {
		// never set
		return false;
	}
	auto &generic_setting = settings[setting_index];
	if (!generic_setting.is_set) {
		return false;
	}
	result_value = generic_setting.value;
	return true;
}

//===--------------------------------------------------------------------===//
// GlobalUserSettings
//===--------------------------------------------------------------------===//
GlobalUserSettings::GlobalUserSettings() : settings_version(0) {
}

GlobalUserSettings::GlobalUserSettings(const GlobalUserSettings &other)
    : settings_map(other.settings_map), extension_parameters(other.extension_parameters),
      settings_version(other.settings_version.load()) {
}

GlobalUserSettings &GlobalUserSettings::operator=(const GlobalUserSettings &other) {
	settings_map = other.settings_map;
	extension_parameters = other.extension_parameters;
	settings_version = other.settings_version.load();
	return *this;
}

void GlobalUserSettings::SetUserSetting(idx_t setting_index, Value target_value) {
	lock_guard<mutex> guard(lock);
	settings_map.SetUserSetting(setting_index, std::move(target_value));
	++settings_version;
}

void GlobalUserSettings::ClearSetting(idx_t setting_index) {
	lock_guard<mutex> guard(lock);
	settings_map.ClearSetting(setting_index);
	++settings_version;
}

bool GlobalUserSettings::IsSet(idx_t setting_index) const {
	lock_guard<mutex> guard(lock);
	return settings_map.IsSet(setting_index);
}

SettingLookupResult GlobalUserSettings::TryGetSetting(idx_t setting_index, Value &result_value) const {
#ifndef __MINGW32__
	// look-up in global settings
	const auto &cache = GetSettings();
	if (cache.settings.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
#else
	lock_guard<mutex> guard(lock);
	if (settings_map.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
#endif

	return SettingLookupResult();
}

bool GlobalUserSettings::HasExtensionOption(const string &name) const {
	lock_guard<mutex> l(lock);
	return extension_parameters.find(name) != extension_parameters.end();
}

idx_t GlobalUserSettings::AddExtensionOption(const string &name, ExtensionOption extension_option) {
	lock_guard<mutex> l(lock);
	const auto new_option = extension_parameters.emplace(make_pair(name, std::move(extension_option)));
	const auto did_insert = new_option.second;
	auto &option = new_option.first->second;

	if (!did_insert) {
		return option.setting_index.GetIndex();
	}

	auto setting_index = GeneratedSettingInfo::MaxSettingIndex + extension_parameters.size() - 1;
	option.setting_index = setting_index;
	++settings_version;
	return setting_index;
}

case_insensitive_map_t<ExtensionOption> GlobalUserSettings::GetExtensionSettings() const {
	lock_guard<mutex> l(lock);
	return extension_parameters;
}

bool GlobalUserSettings::TryGetExtensionOption(const String &name, ExtensionOption &result) const {
	lock_guard<mutex> l(lock);
	auto entry = extension_parameters.find(name.ToStdString());
	if (entry == extension_parameters.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

#ifndef __MINGW32__
CachedGlobalSettings &GlobalUserSettings::GetSettings() const {
	// Cache of global settings - used to allow lock-free access to global settings in a thread-safe manner
	thread_local CachedGlobalSettings current_cache;

	const auto current_version = settings_version.load(std::memory_order_relaxed);
	if (!current_cache.global_user_settings || this != current_cache.global_user_settings.get() ||
	    current_cache.version != current_version) {
		// out-of-date, refresh the cache
		lock_guard<mutex> guard(lock);
		current_cache = CachedGlobalSettings(*this, settings_version, settings_map);
	}
	return current_cache;
}

CachedGlobalSettings::CachedGlobalSettings() : version(0) {
}

CachedGlobalSettings::CachedGlobalSettings(const GlobalUserSettings &global_user_settings_p, idx_t version,
                                           UserSettingsMap settings_p)
    : global_user_settings(global_user_settings_p), version(version), settings(std::move(settings_p)) {
}
#endif

//===--------------------------------------------------------------------===//
// LocalUserSettings
//===--------------------------------------------------------------------===//
LocalUserSettings::~LocalUserSettings() {
}

void LocalUserSettings::SetUserSetting(idx_t setting_index, Value target_value) {
	settings_map.SetUserSetting(setting_index, std::move(target_value));
}

void LocalUserSettings::ClearSetting(idx_t setting_index) {
	settings_map.ClearSetting(setting_index);
}

bool LocalUserSettings::IsSet(idx_t setting_index) const {
	return settings_map.IsSet(setting_index);
}

SettingLookupResult LocalUserSettings::TryGetSetting(const GlobalUserSettings &global_settings, idx_t setting_index,
                                                     Value &result_value) const {
	if (settings_map.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::LOCAL);
	}
	// look-up in global settings
	return global_settings.TryGetSetting(setting_index, result_value);
}

} // namespace duckdb
