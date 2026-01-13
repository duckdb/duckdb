//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/user_settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/setting_info.hpp"

namespace duckdb {

struct UserSettingsMap {
public:
	void SetUserSetting(const String &name, Value target_value);
	void ClearSetting(const String &name);
	bool IsSet(const String &name) const;
	bool TryGetSetting(const String &name, Value &result_value) const;

private:
	case_insensitive_map_t<Value> set_variables;
};

struct CachedGlobalSettings {
	CachedGlobalSettings(idx_t version, UserSettingsMap settings);

	idx_t version;
	UserSettingsMap settings;
};

struct GlobalUserSettings {
public:
	GlobalUserSettings();
	// enable copy constructors
	GlobalUserSettings(const GlobalUserSettings &other);
	GlobalUserSettings &operator=(const GlobalUserSettings &);

	void SetUserSetting(const String &name, Value target_value);
	void ClearSetting(const String &name);
	bool IsSet(const String &name) const;
	SettingLookupResult TryGetSetting(const String &name, Value &result_value) const;
	bool HasExtensionOption(const string &name) const;
	void AddExtensionOption(const string &name, ExtensionOption extension_option);
	case_insensitive_map_t<ExtensionOption> GetExtensionSettings() const;
	bool TryGetExtensionOption(const String &name, ExtensionOption &result) const;
	shared_ptr<CachedGlobalSettings> GetSettings(shared_ptr<CachedGlobalSettings> &cache) const;

private:
	mutable mutex lock;
	UserSettingsMap settings_map;
	//! Extra parameters that can be SET for loaded extensions
	case_insensitive_map_t<ExtensionOption> extension_parameters;
	//! Current version of the settings - incremented when settings are modified
	atomic<idx_t> settings_version;
};

struct LocalUserSettings {
public:
	~LocalUserSettings();

	void SetUserSetting(const String &name, Value target_value);
	void ClearSetting(const String &name);
	bool IsSet(const String &name) const;
	SettingLookupResult TryGetSetting(const GlobalUserSettings &global_settings, const String &name,
	                                  Value &result_value) const;

private:
	UserSettingsMap settings_map;
	mutable shared_ptr<CachedGlobalSettings> global_settings_cache;
};
} // namespace duckdb
