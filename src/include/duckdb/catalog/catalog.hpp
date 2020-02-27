//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <mutex>

namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;

class ClientContext;
class Transaction;

class FunctionExpression;
class SchemaCatalogEntry;
class TableCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
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
	//! Creates a table in the catalog.
	void CreateTable(Transaction &transaction, BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	void CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	void CreateFunction(Transaction &transaction, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	void CreateView(Transaction &transaction, CreateViewInfo *info);
	//! Creates a table in the catalog.
	void CreateSequence(Transaction &transaction, CreateSequenceInfo *info);

	//! Drops an entry from the catalog
	void DropEntry(Transaction &transaction, DropInfo *info);

	//! Alter an existing table in the catalog.
	void AlterTable(ClientContext &context, AlterTableInfo *info);

	//! Returns a pointer to the schema of the specified name. Throws an
	//! exception if it does not exist.
	SchemaCatalogEntry *GetSchema(Transaction &transaction, const string &name = DEFAULT_SCHEMA);
	//! Returns a pointer to the table in the specified schema. Throws an
	//! exception if the schema or the table does not exist.
	TableCatalogEntry *GetTable(ClientContext &context, const string &schema, const string &table);
	//! Gets the sequence, if it exists
	SequenceCatalogEntry *GetSequence(Transaction &transaction, const string &schema, const string &sequence);

	CatalogEntry *GetTableOrView(ClientContext &context, string schema, const string &table);

	//! Returns a pointer to the table function if it exists, or throws an
	//! exception otherwise
	TableFunctionCatalogEntry *GetTableFunction(Transaction &transaction, FunctionExpression *expression);

	//! Returns a pointer to the scalar or aggregate function if it exists, or throws an exception otherwise
	CatalogEntry *GetFunction(Transaction &transaction, const string &schema, const string &name,
	                          bool if_exists = false);

	//! Drops an index from the catalog.
	void DropIndex(Transaction &transaction, DropInfo *info);

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot the schema will be set to DEFAULT_SCHEMA
	static void ParseRangeVar(string input, string &schema, string &name);
private:
	void DropSchema(Transaction &transaction, DropInfo *info);
};
} // namespace duckdb
