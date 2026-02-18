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
#include "duckdb/common/atomic.hpp"

namespace duckdb {

struct GenericSetting {
	GenericSetting() : is_set(false) {
	}

	bool is_set;
	Value value;
};

struct UserSettingsMap {
public:
	void SetUserSetting(idx_t setting_index, Value target_value);
	void ClearSetting(idx_t setting_index);
	bool IsSet(idx_t setting_index) const;
	bool TryGetSetting(idx_t setting_index, Value &result_value) const;

private:
	vector<GenericSetting> settings;
};

#ifndef __MINGW32__
struct GlobalUserSettings;

struct CachedGlobalSettings {
	CachedGlobalSettings();
	CachedGlobalSettings(const GlobalUserSettings &global_user_settings, idx_t version, UserSettingsMap settings);

	optional_ptr<const GlobalUserSettings> global_user_settings;
	idx_t version;
	UserSettingsMap settings;
};
#endif

struct GlobalUserSettings {
public:
	GlobalUserSettings();
	// enable copy constructors
	GlobalUserSettings(const GlobalUserSettings &other);
	GlobalUserSettings &operator=(const GlobalUserSettings &);

	void SetUserSetting(idx_t setting_index, Value target_value);
	void ClearSetting(idx_t setting_index);
	bool IsSet(idx_t setting_index) const;
	SettingLookupResult TryGetSetting(idx_t setting_index, Value &result_value) const;
	bool HasExtensionOption(const string &name) const;
	idx_t AddExtensionOption(const string &name, ExtensionOption extension_option);
	case_insensitive_map_t<ExtensionOption> GetExtensionSettings() const;
	bool TryGetExtensionOption(const String &name, ExtensionOption &result) const;

#ifndef __MINGW32__
	CachedGlobalSettings &GetSettings() const;
#endif

private:
	mutable mutex lock;
	//! Database-global settings
	UserSettingsMap settings_map;
	//! Extra parameters that can be SET for loaded extensions
	case_insensitive_map_t<ExtensionOption> extension_parameters;
	//! Current version of the settings - incremented when settings are modified
	atomic<idx_t> settings_version;
};

struct LocalUserSettings {
public:
	~LocalUserSettings();

	void SetUserSetting(idx_t setting_index, Value target_value);
	void ClearSetting(idx_t setting_index);
	bool IsSet(idx_t setting_index) const;
	SettingLookupResult TryGetSetting(const GlobalUserSettings &global_settings, idx_t setting_index,
	                                  Value &result_value) const;

private:
	//! Client-local settings
	UserSettingsMap settings_map;
};
} // namespace duckdb
