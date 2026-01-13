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

struct UserSettings {
public:
	UserSettings();
	// enable copy constructors
	UserSettings(const UserSettings &other);
	UserSettings &operator=(const UserSettings &);

	void SetUserSetting(const String &name, Value target_value);
	void ClearSetting(const String &name);
	bool IsSet(const String &name) const;
	bool TryGetSetting(const String &name, Value &result_value) const;
	bool HasExtensionOption(const string &name) const;
	void AddExtensionOption(const string &name, ExtensionOption extension_option);
	case_insensitive_map_t<ExtensionOption> GetExtensionSettings() const;
	bool TryGetExtensionOption(const String &name, ExtensionOption &result) const;

private:
	mutable mutex lock;
	case_insensitive_map_t<Value> set_variables;
	//! Extra parameters that can be SET for loaded extensions
	case_insensitive_map_t<ExtensionOption> extension_parameters;
};

} // namespace duckdb
