#pragma once
#include "duckdb/common/string.hpp"

namespace duckdb {
struct ExtensionRepositoryInfo {
	Identifier name;
	bool repository_is_alias = false;
};
} // namespace duckdb
