//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! An index catalog entry
class IndexCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
	    : CatalogEntry(CatalogType::INDEX, catalog, info->index_name), schema(schema) {
		// FIXME: add more information for drop index support
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
};
} // namespace duckdb
