//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/view_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

struct CreateViewInfo;

//! A table catalog entry
class ViewCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInfo *info);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The statement that the view should execute
	unique_ptr<QueryNode> op;
	//! Returns a list of types of the view
	vector<TypeId> GetTypes();
};
} // namespace duckdb
