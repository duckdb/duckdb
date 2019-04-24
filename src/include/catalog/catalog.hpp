//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"

namespace duckdb {
struct CreateSchemaInformation;
struct DropSchemaInformation;
struct CreateTableInformation;
struct DropTableInformation;
struct AlterTableInformation;
struct CreateTableFunctionInformation;
struct CreateScalarFunctionInformation;
struct CreateViewInformation;
struct DropViewInformation;
struct CreateSequenceInformation;
struct DropSequenceInformation;
struct DropIndexInformation;

class FunctionExpression;
class SchemaCatalogEntry;
class TableCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
class ScalarFunctionCatalogEntry;
class StorageManager;

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	Catalog(StorageManager &storage);

	//! Creates a schema in the catalog.
	void CreateSchema(Transaction &transaction, CreateSchemaInformation *info);
	//! Drops a schema in the catalog.
	void DropSchema(Transaction &transaction, DropSchemaInformation *info);

	//! Creates a table in the catalog.
	void CreateTable(Transaction &transaction, CreateTableInformation *info);
	//! Drops a table from the catalog.
	void DropTable(Transaction &transaction, DropTableInformation *info);

	//! Alter an existing table in the catalog.
	void AlterTable(Transaction &transaction, AlterTableInformation *info);
	//! Create a table function in the catalog
	void CreateTableFunction(Transaction &transaction, CreateTableFunctionInformation *info);
	//! Create a scalar function in the catalog
	void CreateScalarFunction(Transaction &transaction, CreateScalarFunctionInformation *info);

	//! Creates a table in the catalog.
	void CreateView(Transaction &transaction, CreateViewInformation *info);
	//! Drops a view in the catalog.
	void DropView(Transaction &transaction, DropViewInformation *info);

	//! Creates a table in the catalog.
	void CreateSequence(Transaction &transaction, CreateSequenceInformation *info);
	//! Drops a view in the catalog.
	void DropSequence(Transaction &transaction, DropSequenceInformation *info);

	//! Returns a pointer to the schema of the specified name. Throws an
	//! exception if it does not exist.
	SchemaCatalogEntry *GetSchema(Transaction &transaction, const string &name = DEFAULT_SCHEMA);
	//! Returns a pointer to the table in the specified schema. Throws an
	//! exception if the schema or the table does not exist.
	TableCatalogEntry *GetTable(Transaction &transaction, const string &schema, const string &table);
	//! Gets the sequence, if it exists
	SequenceCatalogEntry *GetSequence(Transaction &transaction, const string &schema, const string &sequence);

	CatalogEntry *GetTableOrView(Transaction &transaction, const string &schema, const string &table);

	//! Returns a pointer to the table function if it exists, or throws an
	//! exception otherwise
	TableFunctionCatalogEntry *GetTableFunction(Transaction &transaction, FunctionExpression *expression);

	ScalarFunctionCatalogEntry *GetScalarFunction(Transaction &transaction, const string &schema, const string &name);
	//! Drops an index from the catalog.
	void DropIndex(Transaction &transaction, DropIndexInformation *info);
	//! Reference to the storage manager
	StorageManager &storage;

	//! The catalog set holding the schemas
	CatalogSet schemas;
};
} // namespace duckdb
