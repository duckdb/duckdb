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
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

struct DataTableInfo;
class Index;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	//! Create an IndexCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info);
	~IndexCatalogEntry() override;

	Index *index;
	shared_ptr<DataTableInfo> info;
	string sql;
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;

public:
	string ToSQL() override;
	void Serialize(duckdb::MetaBlockWriter &serializer);
	static unique_ptr<CreateIndexInfo> Deserialize(Deserializer &source);
};

} // namespace duckdb
