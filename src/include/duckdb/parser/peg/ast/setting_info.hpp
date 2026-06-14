#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct SettingInfo {
	Identifier name;
	SetScope scope = SetScope::AUTOMATIC; // Default value is defined here
};

} // namespace duckdb
