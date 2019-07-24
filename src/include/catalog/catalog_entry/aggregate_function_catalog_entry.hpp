//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
#include "transaction/transaction.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! A table function in the catalog
class AggregateFunctionCatalogEntry : public CatalogEntry {
public:
	AggregateFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateAggregateFunctionInfo *info)
	    : CatalogEntry(CatalogType::AGGREGATE_FUNCTION, catalog, info->name)
	    , schema(schema)
	    , payload_size(info->payload_size)
	    , initialize(info->initialize)
	    , update(info->update)
	    , finalize(info->finalize)
	    , simple_initialize(info->simple_initialize)
	    , simple_update(info->simple_update)
	    , return_type(info->return_type)
	    , cast_arguments(info->cast_arguments)
	{
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;

	//! The hashed aggregate sizing function
	aggregate_size_t payload_size;
	//! The hashed aggregate initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update function
	aggregate_update_t update;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;

	//! The simple aggregate initialization function (may be null)
	aggregate_simple_initialize_t simple_initialize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;

	//! Function that gives the return type of the aggregate given the input
	//! arguments
	get_return_type_function_t return_type;

	//! Function that returns true if the arguments need to be cast to the return type
	//! arguments
	matches_argument_function_t cast_arguments;
};
} // namespace duckdb
