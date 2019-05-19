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
#include "catalog/dependency_manager.hpp"

#include <mutex>

namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateScalarFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;

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

	//! Reference to the storage manager
	StorageManager &storage;
	//! The catalog set holding the schemas
	CatalogSet schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	DependencyManager dependency_manager;
	//! Write lock for the catalog
	std::mutex write_lock;

public:
	//! Creates a schema in the catalog.
	void CreateSchema(Transaction &transaction, CreateSchemaInfo *info);
	//! Drops a schema in the catalog.
	void DropSchema(Transaction &transaction, DropInfo *info);

	//! Creates a table in the catalog.
	void CreateTable(Transaction &transaction, BoundCreateTableInfo *info);
	//! Drops a table from the catalog.
	void DropTable(Transaction &transaction, DropInfo *info);

	//! Alter an existing table in the catalog.
	void AlterTable(ClientContext &context, AlterTableInfo *info);
	//! Create a table function in the catalog
	void CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info);
	//! Create a scalar function in the catalog
	void CreateScalarFunction(Transaction &transaction, CreateScalarFunctionInfo *info);

	//! Creates a table in the catalog.
	void CreateView(Transaction &transaction, CreateViewInfo *info);
	//! Drops a view in the catalog.
	void DropView(Transaction &transaction, DropInfo *info);

	//! Creates a table in the catalog.
	void CreateSequence(Transaction &transaction, CreateSequenceInfo *info);
	//! Drops a view in the catalog.
	void DropSequence(Transaction &transaction, DropInfo *info);

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
	void DropIndex(Transaction &transaction, DropInfo *info);
};
} // namespace duckdb
