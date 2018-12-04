//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// catalog/view_catalog.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog_entry.hpp"
#include "parser/column_definition.hpp"

#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

//! A table catalog entry
class ViewCatalogEntry : public CatalogEntry {
  public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                 CreateViewInformation *info);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The statement that the view should execute
	std::unique_ptr<QueryNode> op;

	//! Returns a list of types of the view
	std::vector<TypeId> GetTypes();
};
} // namespace duckdb
