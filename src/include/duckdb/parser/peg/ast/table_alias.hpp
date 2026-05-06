#pragma once
#include "duckdb/common/common.hpp"

namespace duckdb {
struct TableAlias {
	string name;
	vector<string> column_name_alias;
};
} // namespace duckdb
