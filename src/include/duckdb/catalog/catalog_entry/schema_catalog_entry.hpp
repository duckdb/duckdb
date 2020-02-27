//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/schema_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"

namespace duckdb {
class FunctionExpression;

class StandardEntry;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class SequenceCatalogEntry;

enum class OnCreateConflict : uint8_t;

struct AlterTableInfo;
class ClientContext;
struct CreateIndexInfo;
struct CreateTableFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct BoundCreateTableInfo;
struct CreateSequenceInfo;
struct CreateSchemaInfo;
struct CreateTableFunctionInfo;
struct DropInfo;

class Transaction;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
public:
	SchemaCatalogEntry(Catalog *catalog, string name);

	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;
	//! The catalog set holding the sequences
	CatalogSet sequences;
public:
	//! Returns a pointer to a table of the given name. Throws an exception if
	//! the table does not exist.
	TableCatalogEntry *GetTable(Transaction &transaction, const string &table);
	TableCatalogEntry *GetTableOrNull(Transaction &transaction, const string &table);
	CatalogEntry *GetTableOrView(Transaction &transaction, const string &table);

	//! Creates a table with the given name in the schema
	void CreateTable(Transaction &transaction, BoundCreateTableInfo *info);

	//! Creates a view with the given name in the schema
	void CreateView(Transaction &transaction, CreateViewInfo *info);
	//! Drops a view with the given name in the schema
	void DropView(Transaction &transaction, DropInfo *info);

	//! Creates a sequence with the given name in the schema
	void CreateSequence(Transaction &transaction, CreateSequenceInfo *info);
	//! Drops a sequence with the given name in the schema
	void DropSequence(Transaction &transaction, DropInfo *info);

	//! Creates an index with the given name in the schema
	bool CreateIndex(Transaction &transaction, CreateIndexInfo *info);
	//! Drops a index with the given name
	void DropIndex(Transaction &transaction, DropInfo *info);
	//! Drops a table with the given name
	void DropTable(Transaction &transaction, DropInfo *info);

	//! Alters a table
	void AlterTable(ClientContext &context, AlterTableInfo *info);

	//! Gets a table function matching the given function expression
	TableFunctionCatalogEntry *GetTableFunction(Transaction &transaction, FunctionExpression *expression);
	//! Create a table function within the given schema
	void CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info);
	//! Create a scalar or aggregate function within the given schema
	void CreateFunction(Transaction &transaction, CreateFunctionInfo *info);

	//! Gets a scalar function with the given name
	CatalogEntry *GetFunction(Transaction &transaction, const string &name, bool if_exists = false);
	//! Gets the sequence with the given name
	SequenceCatalogEntry *GetSequence(Transaction &transaction, const string &name);

	//! Serialize the meta information of the SchemaCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateSchemaInfo
	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &source);
private:
	//! Add a catalog entry to this schema
	bool AddEntry(Transaction &transaction, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies = {});

	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb
