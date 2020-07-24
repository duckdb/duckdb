//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class Catalog;
struct CreateCopyFunctionInfo;

//! A table function in the catalog
class CopyFunctionCatalogEntry : public StandardEntry {
public:
	CopyFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateCopyFunctionInfo *info);

	//! The copy function
	CopyFunction function;
};
} // namespace duckdb
