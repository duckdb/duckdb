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
	DUCKDB_API explicit CatalogSearchPath(ClientContext &client_p);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	DUCKDB_API void Set(const string &new_value, bool is_set_schema);
	DUCKDB_API void Set(vector<string> &new_paths, bool is_set_schema = false);

	DUCKDB_API const vector<string> &Get();
	DUCKDB_API const vector<string> &GetSetPaths() {
		return set_paths;
	}
	DUCKDB_API const string &GetDefault();
	DUCKDB_API const string &GetOrDefault(const string &name);

private:
	static vector<string> ParsePaths(const string &value);

	void SetPaths(vector<string> new_paths);

private:
	ClientContext &context;
	vector<string> paths;
	//! Only the paths that were explicitly set (minus the always included paths)
	vector<string> set_paths;
};

} // namespace duckdb
