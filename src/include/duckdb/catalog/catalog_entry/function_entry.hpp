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
	    : StandardEntry(type, schema, catalog, info.GetFunctionName()) {
		descriptions = std::move(info.descriptions);
		alias_of = std::move(info.alias_of);
		this->dependencies = info.dependencies;
		this->internal = info.internal;
		this->extension_name = info.extension_name;
	}

	Identifier alias_of;
	vector<FunctionDescription> descriptions;

	//! Watched by a binder that holds on to this entry while it binds the function's arguments: binding an argument
	//! can autoload an extension that adds overloads to this function, which replaces the entry and destroys this one
	weak_ptr<const bool> GetLivenessToken() const {
		return liveness;
	}

private:
	shared_ptr<const bool> liveness = make_shared_ptr<const bool>(true);
};
} // namespace duckdb
