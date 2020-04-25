//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/collate_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"

namespace duckdb {

//! A collation catalog entry
class CollateCatalogEntry : public StandardEntry {
public:
	CollateCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateCollationInfo *info)
	    : StandardEntry(CatalogType::COLLATION, schema, catalog, info->name), function(info->function), combinable(info->combinable) {
	}

	ScalarFunction function;
	bool combinable;
};
} // namespace duckdb
