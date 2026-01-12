#include "duckdb/main/user_settings.hpp"

namespace duckdb {

UserSettings::UserSettings() {
}

UserSettings::UserSettings(const UserSettings &other) : set_variables(other.set_variables) {
}

UserSettings &UserSettings::operator=(const UserSettings &other) {
	set_variables = other.set_variables;
	return *this;
}

void UserSettings::SetUserSetting(const String &name, Value target_value) {
	lock_guard<mutex> guard(lock);
	set_variables[name.ToStdString()] = std::move(target_value);
}

void UserSettings::ClearSetting(const String &name) {
	lock_guard<mutex> guard(lock);
	set_variables.erase(name.ToStdString());
}

bool UserSettings::IsSet(const String &name) const {
	lock_guard<mutex> guard(lock);
	return set_variables.find(name.ToStdString()) != set_variables.end();
}

bool UserSettings::TryGetSetting(const String &name, Value &result_value) const {
	lock_guard<mutex> guard(lock);
	auto entry = set_variables.find(name.ToStdString());
	if (entry == set_variables.end()) {
		return false;
	}
	result_value = entry->second;
	return true;
}

} // namespace duckdb
