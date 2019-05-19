//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "common/types/statistics.hpp"
#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "planner/bound_constraint.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class ColumnStatistics;
class DataTable;
class SchemaCatalogEntry;
struct CreateTableInfo;
struct BoundCreateTableInfo;

//! A table catalog entry
class TableCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
	                  std::shared_ptr<DataTable> inherited_storage = nullptr);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! A reference to the underlying storage unit used for this table
	std::shared_ptr<DataTable> storage;
	//! A list of columns that are part of this table
	vector<ColumnDefinition> columns;
	//! A list of constraints that are part of this table
	vector<unique_ptr<Constraint>> constraints;
	//! A list of constraints that are part of this table
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! A map of column name to column index
	unordered_map<string, column_t> name_map;

public:
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	//! Returns whether or not a column with the given name exists
	bool ColumnExists(const string &name);
	//! Returns the statistics of the oid-th column. Throws an exception if the
	//! access is out of range.
	ColumnStatistics &GetStatistics(column_t oid);
	//! Returns a reference to the column of the specified name. Throws an
	//! exception if the column does not exist.
	ColumnDefinition &GetColumn(const string &name);
	//! Returns a list of types of the table
	vector<TypeId> GetTypes();
	//! Returns a list of types of the specified columns of the table
	vector<TypeId> GetTypes(const vector<column_t> &column_ids);

	//! Serialize the meta information of the TableCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);
};
} // namespace duckdb
