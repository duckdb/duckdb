//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/scalar_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "transaction/transaction.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public CatalogEntry {
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info)
	    : CatalogEntry(CatalogType::SCALAR_FUNCTION, catalog, info->name), schema(schema), function(info->function),
	      matches(info->matches), return_type(info->return_type), bind(info->bind), dependency(info->dependency),
	      has_side_effects(info->has_side_effects) {
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The main scalar function to execute
	scalar_function_t function;
	//! Function that checks whether or not a set of arguments matches
	matches_argument_function_t matches;
	//! Function that gives the return type of the function given the input
	//! arguments
	get_return_type_function_t return_type;
	//! The bind function
	bind_scalar_function_t bind;
	//! The dependency function
	dependency_function_t dependency;

	bool has_side_effects;
};
} // namespace duckdb
