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

private:
	mutable mutex lock;
	case_insensitive_map_t<Value> set_variables;
};

} // namespace duckdb
