//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"

namespace duckdb {

struct DataTableInfo;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::INDEX_ENTRY;
	static constexpr const char *Name = "index";

public:
	//! Create an IndexCatalogEntry
	IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info);

	string sql;
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	case_insensitive_map_t<Value> options;

public:
	//! Returns the CreateIndexInfo
	unique_ptr<CreateInfo> GetInfo() const override;
	//! Returns the original CREATE INDEX SQL
	string ToSQL() const override;

	virtual string GetSchemaName() const = 0;
	virtual string GetTableName() const = 0;
};

} // namespace duckdb
