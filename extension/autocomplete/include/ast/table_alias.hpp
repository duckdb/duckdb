#pragma once

namespace duckdb {
struct TableAlias {
	string name;
	vector<string> column_name_alias;
};
} // namespace duckdb
