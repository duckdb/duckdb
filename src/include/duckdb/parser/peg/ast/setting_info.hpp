#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct SettingInfo {
	string name;
	SetScope scope = SetScope::AUTOMATIC; // Default value is defined here
};

} // namespace duckdb
