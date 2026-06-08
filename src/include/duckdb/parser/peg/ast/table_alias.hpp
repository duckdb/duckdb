#pragma once
#include "duckdb/common/common.hpp"

namespace duckdb {
struct TableAlias {
	Identifier name;
	vector<Identifier> column_name_alias;
};
} // namespace duckdb
