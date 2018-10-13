//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/view_catalog.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/abstract_catalog.hpp"
#include "catalog/column_definition.hpp"

#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

//! A table catalog entry
class ViewCatalogEntry : public AbstractCatalogEntry {
  public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                 CreateViewInformation *info);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The statement that the view should execute
	std::unique_ptr<SelectStatement> op;

	//! Returns a list of types of the view
	std::vector<TypeId> GetTypes();
};
} // namespace duckdb
