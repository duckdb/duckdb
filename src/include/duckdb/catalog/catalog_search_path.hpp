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

	const vector<string> &Get();

private:
	static vector<string> ParsePaths(const string &value);
	ClientContext &context;
	string last_value;
	vector<string> paths;
};

} // namespace duckdb
