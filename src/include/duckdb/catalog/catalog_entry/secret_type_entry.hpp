//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/secret_type_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

//! A Secret type in the catalog
struct SecretTypeEntry : public InCatalogEntry {
	SecretTypeEntry(Catalog &catalog, SecretType &type)
	    : InCatalogEntry(CatalogType::SECRET_ENTRY, catalog, type.name), type(type) {
		internal = true;
	}

	SecretType type;
};
} // namespace duckdb
