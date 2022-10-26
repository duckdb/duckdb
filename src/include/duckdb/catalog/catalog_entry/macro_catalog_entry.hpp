//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! A macro function in the catalog
class MacroCatalogEntry : public StandardEntry {
public:
	MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info);
	//! The macro function
	unique_ptr<MacroFunction> function;

public:
	//! Serialize the meta information
	virtual void Serialize(Serializer &serializer) = 0;

	string ToSQL() override {
		return function->ToSQL(schema->name, name);
	}
};

} // namespace duckdb
