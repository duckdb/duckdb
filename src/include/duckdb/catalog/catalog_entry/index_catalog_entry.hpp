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

namespace duckdb {

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
	    : StandardEntry(CatalogType::INDEX, schema, catalog, info->index_name) {
		// FIXME: add more information for drop index support
	}
};

} // namespace duckdb
