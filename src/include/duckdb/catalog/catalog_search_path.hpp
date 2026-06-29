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
	CatalogSearchEntry(Identifier catalog, Identifier schema);

public:
	const Identifier &GetCatalog() const {
		return catalog;
	}
	void SetCatalog(Identifier new_catalog) {
		catalog = std::move(new_catalog);
	}
	const Identifier &GetSchema() const {
		return schema;
	}
	void SetSchema(Identifier new_schema) {
		schema = std::move(new_schema);
	}

	string ToString() const;
	static string ListToString(const vector<CatalogSearchEntry> &input);
	static CatalogSearchEntry Parse(const string &input);
	static vector<CatalogSearchEntry> ParseList(const string &input);

private:
	static CatalogSearchEntry ParseInternal(const string &input, idx_t &pos);
	static string WriteOptionallyQuoted(const Identifier &input);

private:
	Identifier catalog;
	Identifier schema;
};

enum class CatalogSetPathType { SET_SCHEMA, SET_SCHEMAS, SET_DIRECTLY };

//! The schema search path, in order by which entries are searched if no schema entry is provided
class CatalogSearchPath {
public:
	DUCKDB_API explicit CatalogSearchPath(ClientContext &client_p);
	DUCKDB_API CatalogSearchPath(ClientContext &client_p, vector<CatalogSearchEntry> entries);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	DUCKDB_API void Set(CatalogSearchEntry new_value, CatalogSetPathType set_type);
	DUCKDB_API void Set(vector<CatalogSearchEntry> new_paths, CatalogSetPathType set_type);
	DUCKDB_API void Reset();
	DUCKDB_API void RefreshSetPaths();

	DUCKDB_API vector<CatalogSearchEntry> Get() const;
	const vector<CatalogSearchEntry> &GetSetPaths() const {
		return set_paths;
	}
	DUCKDB_API const CatalogSearchEntry &GetDefault() const;
	//! FIXME: this method is deprecated
	DUCKDB_API Identifier GetDefaultSchema(const Identifier &catalog) const;
	DUCKDB_API Identifier GetDefaultSchema(ClientContext &context, const Identifier &catalog) const;
	DUCKDB_API Identifier GetDefaultCatalog(const Identifier &schema) const;

	DUCKDB_API vector<Identifier> GetSchemasForCatalog(const Identifier &catalog) const;
	DUCKDB_API vector<Identifier> GetCatalogsForSchema(const Identifier &schema) const;

	DUCKDB_API bool SchemaInSearchPath(ClientContext &context, const Identifier &catalog_name,
	                                   const Identifier &schema_name) const;

private:
	//! Set paths without checking if they exist
	void SetPathsInternal(vector<CatalogSearchEntry> new_paths);
	string GetSetName(CatalogSetPathType set_type);

private:
	ClientContext &context;
	vector<CatalogSearchEntry> paths;
	//! Only the paths that were explicitly set (minus the always included paths)
	vector<CatalogSearchEntry> set_paths;
};

} // namespace duckdb
