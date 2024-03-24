//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/function_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

//! An aggregate function in the catalog
class FunctionEntry : public StandardEntry {
public:
	FunctionEntry(CatalogType type, Catalog &catalog, SchemaCatalogEntry &schema, CreateFunctionInfo &info)
	    : StandardEntry(type, schema, catalog, info.name) {
		description = std::move(info.description);
		parameter_names = std::move(info.parameter_names);
		example = std::move(info.example);
	}

	//! The description (if any)
	string description;
	//! Parameter names (if any)
	vector<string> parameter_names;
	//! The example (if any)
	string example;
};
} // namespace duckdb
