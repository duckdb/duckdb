#include "duckdb/main/user_settings.hpp"

namespace duckdb {

void UserSettingsMap::SetUserSetting(idx_t setting_index, Value target_value) {
	set_variables[setting_index] = std::move(target_value);
}

void UserSettingsMap::ClearSetting(idx_t setting_index) {
	set_variables.erase(setting_index);
}

bool UserSettingsMap::IsSet(idx_t setting_index) const {
	return set_variables.find(setting_index) != set_variables.end();
}

bool UserSettingsMap::TryGetSetting(idx_t setting_index, Value &result_value) const {
	auto entry = set_variables.find(setting_index);
	if (entry == set_variables.end()) {
		return false;
	}
	result_value = entry->second;
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
	lock_guard<mutex> guard(lock);
	if (!settings_map.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult();
	}
	return SettingLookupResult(SettingScope::GLOBAL);
}

bool GlobalUserSettings::HasExtensionOption(const string &name) const {
	lock_guard<mutex> l(lock);
	return extension_parameters.find(name) != extension_parameters.end();
}

idx_t GlobalUserSettings::AddExtensionOption(const string &name, ExtensionOption extension_option) {
	lock_guard<mutex> l(lock);
	auto setting_index = GeneratedSettingInfo::MaxSettingIndex + extension_parameters.size();
	extension_option.setting_index = setting_index;
	extension_parameters.insert(make_pair(name, std::move(extension_option)));
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

shared_ptr<CachedGlobalSettings> GlobalUserSettings::GetSettings(shared_ptr<CachedGlobalSettings> &cache) const {
	auto current_cache = cache.atomic_load(std::memory_order_relaxed);
	auto current_version = settings_version.load(std::memory_order_relaxed);
	if (current_cache && current_cache->version == current_version) {
		// we have a cached version and it is up to date - done
		return current_cache;
	}
	lock_guard<mutex> guard(lock);
	// check if another thread updated the cache while we were waiting for the lock
	if (cache && current_version == cache->version) {
		// already written - load
		return cache;
	}
	auto new_cache = make_shared_ptr<CachedGlobalSettings>(settings_version, settings_map);
	cache.atomic_store(new_cache);
	return new_cache;
}

CachedGlobalSettings::CachedGlobalSettings(idx_t version, UserSettingsMap settings_p)
    : version(version), settings(std::move(settings_p)) {
}

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
	auto cache = global_settings.GetSettings(global_settings_cache);
	if (cache->settings.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
	return SettingLookupResult();
}

} // namespace duckdb
