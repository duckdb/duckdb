//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
struct CreateWindowFunctionInfo;

//! An aggregate function in the catalog
class WindowFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::WINDOW_FUNCTION_ENTRY;
	static constexpr const char *Name = "window function";

	static vector<unique_ptr<WindowFunctionCatalogEntry>> GetEntries(ClientContext &context);

public:
	WindowFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateWindowFunctionInfo &info);

	//! The window functions
	WindowFunctionSet functions;
};
} // namespace duckdb
