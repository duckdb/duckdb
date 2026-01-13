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

	void SetUserSetting(idx_t setting_index, Value target_value);
	void ClearSetting(idx_t setting_index);
	bool IsSet(idx_t setting_index) const;
	SettingLookupResult TryGetSetting(idx_t setting_index, Value &result_value) const;
	bool HasExtensionOption(const string &name) const;
	idx_t AddExtensionOption(const string &name, ExtensionOption extension_option);
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

	void SetUserSetting(idx_t setting_index, Value target_value);
	void ClearSetting(idx_t setting_index);
	bool IsSet(idx_t setting_index) const;
	SettingLookupResult TryGetSetting(const GlobalUserSettings &global_settings, idx_t setting_index,
	                                  Value &result_value) const;

private:
	UserSettingsMap settings_map;
	mutable shared_ptr<CachedGlobalSettings> global_settings_cache;
};
} // namespace duckdb
