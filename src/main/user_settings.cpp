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

UserSettings::UserSettings() {
}

UserSettings::UserSettings(const UserSettings &other)
    : settings_map(other.settings_map), extension_parameters(other.extension_parameters) {
}

UserSettings &UserSettings::operator=(const UserSettings &other) {
	settings_map = other.settings_map;
	extension_parameters = other.extension_parameters;
	return *this;
}

void UserSettings::SetUserSetting(const String &name, Value target_value) {
	lock_guard<mutex> guard(lock);
	settings_map.SetUserSetting(name, std::move(target_value));
}

void UserSettings::ClearSetting(const String &name) {
	lock_guard<mutex> guard(lock);
	settings_map.ClearSetting(name);
}

bool UserSettings::IsSet(const String &name) const {
	lock_guard<mutex> guard(lock);
	return settings_map.IsSet(name);
}

bool UserSettings::TryGetSetting(const String &name, Value &result_value) const {
	lock_guard<mutex> guard(lock);
	return settings_map.TryGetSetting(name, result_value);
}

bool UserSettings::HasExtensionOption(const string &name) const {
	lock_guard<mutex> l(lock);
	return extension_parameters.find(name) != extension_parameters.end();
}

void UserSettings::AddExtensionOption(const string &name, ExtensionOption extension_option) {
	lock_guard<mutex> l(lock);
	extension_parameters.insert(make_pair(name, std::move(extension_option)));
}

case_insensitive_map_t<ExtensionOption> UserSettings::GetExtensionSettings() const {
	lock_guard<mutex> l(lock);
	return extension_parameters;
}

bool UserSettings::TryGetExtensionOption(const String &name, ExtensionOption &result) const {
	lock_guard<mutex> l(lock);
	auto entry = extension_parameters.find(name.ToStdString());
	if (entry == extension_parameters.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

} // namespace duckdb
