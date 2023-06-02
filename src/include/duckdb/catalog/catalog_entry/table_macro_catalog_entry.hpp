//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

//! A macro function in the catalog
class TableMacroCatalogEntry : public MacroCatalogEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TABLE_MACRO_ENTRY;
	static constexpr const char *Name = "table macro function";

public:
	TableMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);
};

} // namespace duckdb
