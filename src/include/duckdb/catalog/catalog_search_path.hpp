//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_search_path.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class ClientContext;

//! The schema search path, in order by which entries are searched if no schema entry is provided
class CatalogSearchPath {
public:
	explicit CatalogSearchPath(ClientContext &client_p);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	void Set(const string &new_value, bool is_set_schema);
	const vector<string> &Get();
	const string &GetDefault();
	const string &GetOrDefault(const string &name);

private:
	static vector<string> ParsePaths(const string &value);

	void SetPaths(vector<string> new_paths);
private:
	ClientContext &context;
	string last_value;
	vector<string> paths;
};

} // namespace duckdb
