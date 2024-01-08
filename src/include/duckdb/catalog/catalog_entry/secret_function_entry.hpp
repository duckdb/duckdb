//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/secret_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

//! A Secret function in the catalog
class CreateSecretFunctionEntry : public InCatalogEntry {
public:
	CreateSecretFunctionEntry(Catalog &catalog, CreateSecretFunctionSet &function_set, const string &name)
	    : InCatalogEntry(CatalogType::SECRET_FUNCTION_ENTRY, catalog, name), function_set(function_set) {
		internal = true;
	}

	CreateSecretFunctionSet function_set;
};

} // namespace duckdb
