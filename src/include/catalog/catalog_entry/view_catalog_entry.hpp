//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/view_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "common/types/statistics.hpp"
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

class ColumnStatistics;
class DataTable;
class SchemaCatalogEntry;

//! A view catalog entry
class ViewCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInformation *info);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;

	//! Serialize the meta information of the TableCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	static unique_ptr<CreateViewInformation> Deserialize(Deserializer &source);

	unique_ptr<QueryNode> query;

private:
	void Initialize(CreateViewInformation *info);
};
} // namespace duckdb
