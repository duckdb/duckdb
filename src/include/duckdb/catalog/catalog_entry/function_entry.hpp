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

//! A function in the catalog
class FunctionEntry : public StandardEntry {
public:
	FunctionEntry(CatalogType type, Catalog &catalog, SchemaCatalogEntry &schema, CreateFunctionInfo &info)
	    : StandardEntry(type, schema, catalog, info.name) {
		descriptions = std::move(info.descriptions);
		this->dependencies = info.dependencies;
		this->internal = info.internal;
	}

	vector<FunctionDescription> descriptions;
};
} // namespace duckdb
