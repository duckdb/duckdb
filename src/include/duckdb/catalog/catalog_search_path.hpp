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

struct CatalogSearchEntry {
	CatalogSearchEntry(string catalog, string schema);

	string catalog;
	string schema;

public:
	string ToString() const;
	static string ListToString(const vector<CatalogSearchEntry> &input);
	static CatalogSearchEntry Parse(const string &input);
	static vector<CatalogSearchEntry> ParseList(const string &input);

private:
	static CatalogSearchEntry ParseInternal(const string &input, idx_t &pos);
	static string WriteOptionallyQuoted(const string &input);
};

//! The schema search path, in order by which entries are searched if no schema entry is provided
class CatalogSearchPath {
public:
	DUCKDB_API explicit CatalogSearchPath(ClientContext &client_p);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	DUCKDB_API void Set(CatalogSearchEntry new_value, bool is_set_schema);
	DUCKDB_API void Set(vector<CatalogSearchEntry> new_paths, bool is_set_schema = false);
	DUCKDB_API void Reset();

	DUCKDB_API const vector<CatalogSearchEntry> &Get();
	DUCKDB_API const vector<CatalogSearchEntry> &GetSetPaths() {
		return set_paths;
	}
	DUCKDB_API const CatalogSearchEntry &GetDefault();
	DUCKDB_API string GetDefaultSchema(const string &catalog);
	DUCKDB_API string GetDefaultCatalog(const string &schema);

	DUCKDB_API vector<string> GetSchemasForCatalog(const string &catalog);
	DUCKDB_API vector<string> GetCatalogsForSchema(const string &schema);

private:
	void SetPaths(vector<CatalogSearchEntry> new_paths);

private:
	ClientContext &context;
	vector<CatalogSearchEntry> paths;
	//! Only the paths that were explicitly set (minus the always included paths)
	vector<CatalogSearchEntry> set_paths;
};

} // namespace duckdb
