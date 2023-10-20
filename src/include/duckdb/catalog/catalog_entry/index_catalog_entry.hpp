//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"

namespace duckdb {

struct DataTableInfo;
class Index;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::INDEX_ENTRY;
	static constexpr const char *Name = "index";

public:
	//! Create an IndexCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info);

	optional_ptr<Index> index;
	string sql;
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	case_insensitive_map_t<Value> options;

public:
	unique_ptr<CreateInfo> GetInfo() const override;
	string ToSQL() const override;

	virtual string GetSchemaName() const = 0;
	virtual string GetTableName() const = 0;
};

} // namespace duckdb
