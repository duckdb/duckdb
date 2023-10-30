//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

class Catalog;
struct CreateSecretFunctionInfo;

//! A create secret function in the catalog
class CreateSecretFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::CREATE_SECRET_FUNCTION_ENTRY;
	static constexpr const char *Name = "create secret function";

public:
	CreateSecretFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateSecretFunctionInfo &info);

	//! The pragma functions
	CreateSecretFunctionSet functions;
};
} // namespace duckdb
