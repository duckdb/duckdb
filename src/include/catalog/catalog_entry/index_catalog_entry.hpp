//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/catalog_entry/index_catalog_entry.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "parser/parsed_data.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! A table catalog entry
class IndexCatalogEntry : public CatalogEntry {
  public:
	//! Create a real TableCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                  CreateIndexInformation *info)
	    : CatalogEntry(CatalogType::INDEX, catalog, info->index_name),
	      schema(schema) {
		// FIXME: add more information for drop index support
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
};
} // namespace duckdb
