//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_function_info.hpp"

namespace duckdb {

//! A macro function in the catalog
class MacroFunctionCatalogEntry : public StandardEntry {
public:
	MacroFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroFunctionInfo *info)
	    : StandardEntry(CatalogType::MACRO_ENTRY, schema, catalog, info->name), function(move(info->function)) {
	}

	//! The macro function
	unique_ptr<MacroFunction> function;

public:
    //! Serialize the meta information of the MacroFunctionCatalogEntry a serializer
    virtual void Serialize(Serializer &serializer);
    //! Deserializes to a CreateMacroFunctionInfo
    static unique_ptr<CreateMacroFunctionInfo> Deserialize(Deserializer &source);
};
} // namespace duckdb
