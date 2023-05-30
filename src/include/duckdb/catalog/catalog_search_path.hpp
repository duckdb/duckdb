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

enum class CatalogSetPathType { SET_SCHEMA, SET_SCHEMAS };

//! The schema search path, in order by which entries are searched if no schema entry is provided
class CatalogSearchPath {
public:
	DUCKDB_API explicit CatalogSearchPath(ClientContext &client_p);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	DUCKDB_API void Set(CatalogSearchEntry new_value, CatalogSetPathType set_type);
	DUCKDB_API void Set(vector<CatalogSearchEntry> new_paths, CatalogSetPathType set_type);
	DUCKDB_API void Reset();

	DUCKDB_API const vector<CatalogSearchEntry> &Get();
	const vector<CatalogSearchEntry> &GetSetPaths() {
		return set_paths;
	}
	DUCKDB_API const CatalogSearchEntry &GetDefault();
	DUCKDB_API string GetDefaultSchema(const string &catalog);
	DUCKDB_API string GetDefaultCatalog(const string &schema);

	DUCKDB_API vector<string> GetSchemasForCatalog(const string &catalog);
	DUCKDB_API vector<string> GetCatalogsForSchema(const string &schema);

	DUCKDB_API bool SchemaInSearchPath(ClientContext &context, const string &catalog_name, const string &schema_name);

private:
	void SetPaths(vector<CatalogSearchEntry> new_paths);

	string GetSetName(CatalogSetPathType set_type);

private:
	ClientContext &context;
	vector<CatalogSearchEntry> paths;
	//! Only the paths that were explicitly set (minus the always included paths)
	vector<CatalogSearchEntry> set_paths;
};

} // namespace duckdb
