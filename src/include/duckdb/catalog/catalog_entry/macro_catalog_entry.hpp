//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! A macro function in the catalog
class MacroCatalogEntry : public FunctionEntry {
public:
	MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);

	//! The macro function
	unique_ptr<MacroFunction> function;
	//! The inherent dependencies of the function
	DependencyList dependencies;

public:
	unique_ptr<CreateInfo> GetInfo() const override;

	string ToSQL() const override {
		return function->ToSQL(schema.name, name);
	}

	DependencyList InherentDependencies() override {
		return dependencies;
	}
};

} // namespace duckdb
