//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_macro_function_info.hpp"

namespace duckdb {

class Catalog;
struct CreateMacroFunctionInfo;

//! A macro function in the catalog
class MacroFunctionCatalogEntry : public StandardEntry {
public:
	MacroFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroFunctionInfo *info)
	    : StandardEntry(CatalogType::MACRO_FUNCTION_ENTRY, schema, catalog, info->name),
	      arguments(move(info->arguments)), function(move(info->function)) {
	}

	//! The macro arguments
	vector<unique_ptr<ParsedExpression>> arguments;
	//! The macro function
	unique_ptr<ParsedExpression> function;
};
} // namespace duckdb
