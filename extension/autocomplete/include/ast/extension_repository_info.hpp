#pragma once
#include "duckdb/common/string.hpp"

namespace duckdb {
struct ExtensionRepositoryInfo {
	string name;
	bool repository_is_alias = false;
};
} // namespace duckdb
