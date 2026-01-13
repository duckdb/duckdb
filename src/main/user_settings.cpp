#include "duckdb/main/user_settings.hpp"

namespace duckdb {

void UserSettingsMap::SetUserSetting(const String &name, Value target_value) {
	set_variables[name.ToStdString()] = std::move(target_value);
}

void UserSettingsMap::ClearSetting(const String &name) {
	set_variables.erase(name.ToStdString());
}

bool UserSettingsMap::IsSet(const String &name) const {
	return set_variables.find(name.ToStdString()) != set_variables.end();
}

bool UserSettingsMap::TryGetSetting(const String &name, Value &result_value) const {
	auto entry = set_variables.find(name.ToStdString());
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

void GlobalUserSettings::SetUserSetting(const String &name, Value target_value) {
	lock_guard<mutex> guard(lock);
	settings_map.SetUserSetting(name, std::move(target_value));
	++settings_version;
}

void GlobalUserSettings::ClearSetting(const String &name) {
	lock_guard<mutex> guard(lock);
	settings_map.ClearSetting(name);
	++settings_version;
}

bool GlobalUserSettings::IsSet(const String &name) const {
	lock_guard<mutex> guard(lock);
	return settings_map.IsSet(name);
}

SettingLookupResult GlobalUserSettings::TryGetSetting(const String &name, Value &result_value) const {
	lock_guard<mutex> guard(lock);
	if (!settings_map.TryGetSetting(name, result_value)) {
		return SettingLookupResult();
	}
	return SettingLookupResult(SettingScope::GLOBAL);
}

bool GlobalUserSettings::HasExtensionOption(const string &name) const {
	lock_guard<mutex> l(lock);
	return extension_parameters.find(name) != extension_parameters.end();
}

void GlobalUserSettings::AddExtensionOption(const string &name, ExtensionOption extension_option) {
	lock_guard<mutex> l(lock);
	extension_parameters.insert(make_pair(name, std::move(extension_option)));
	++settings_version;
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

void LocalUserSettings::SetUserSetting(const String &name, Value target_value) {
	settings_map.SetUserSetting(name, std::move(target_value));
}

void LocalUserSettings::ClearSetting(const String &name) {
	settings_map.ClearSetting(name);
}

bool LocalUserSettings::IsSet(const String &name) const {
	return settings_map.IsSet(name);
}

SettingLookupResult LocalUserSettings::TryGetSetting(const GlobalUserSettings &global_settings, const String &name,
                                                     Value &result_value) const {
	if (settings_map.TryGetSetting(name, result_value)) {
		return SettingLookupResult(SettingScope::LOCAL);
	}
	// look-up in global settings
	auto cache = global_settings.GetSettings(global_settings_cache);
	if (cache->settings.TryGetSetting(name, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
	return SettingLookupResult();
}

} // namespace duckdb
