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
#include "duckdb/catalog/catalog_entry/base_macro_catalog_entry.hpp"

namespace duckdb {

//! A macro function in the catalog
class TableMacroCatalogEntry : public BaseMacroCatalogEntry {
public:
	TableMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info);

public:
	//! Serialize the meta information of the MacroCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateMacroInfo
	static unique_ptr<CreateMacroInfo> Deserialize(Deserializer &source);
	// static unique_ptr<CreateMacroInfo> Deserialize2(Deserializer &main_source);
};

} // namespace duckdb
