//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/query_error_context.hpp"

#include <functional>

namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;
struct CreatePragmaFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;
struct CreateCollationInfo;

class ClientContext;
class Transaction;

class AggregateFunctionCatalogEntry;
class CollateCatalogEntry;
class SchemaCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
class CopyFunctionCatalogEntry;
class PragmaFunctionCatalogEntry;
class StorageManager;
class CatalogSet;
class DependencyManager;

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	Catalog(StorageManager &storage);
	~Catalog();

	//! Reference to the storage manager
	StorageManager &storage;
	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;

public:
	//! Get the ClientContext from the Catalog
	static Catalog &GetCatalog(ClientContext &context);

	//! Creates a schema in the catalog.
	CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a collation in the catalog
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Drops an entry from the catalog
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA,
	                              QueryErrorContext error_context = QueryErrorContext());
	//! Scans all the schemas in the system one-by-one, invoking the callback for each entry
	void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback);
	//! Gets the "schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not exist,
	//! otherwise an exception is thrown
	CatalogEntry *GetEntry(ClientContext &context, CatalogType type, string schema, const string &name,
	                       bool if_exists = false, QueryErrorContext error_context = QueryErrorContext());

	template <class T>
	T *GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists = false, QueryErrorContext error_context = QueryErrorContext());

	//! Alter an existing entry in the catalog.
	void Alter(ClientContext &context, AlterInfo *info);

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to DEFAULT_SCHEMA
	static void ParseRangeVar(string input, string &schema, string &name);

private:
	void DropSchema(ClientContext &context, DropInfo *info);
};

template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists, QueryErrorContext error_context);
template <>
ViewCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists, QueryErrorContext error_context);
template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists, QueryErrorContext error_context);
template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                             bool if_exists, QueryErrorContext error_context);
template <>
CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                            bool if_exists, QueryErrorContext error_context);
template <>
PragmaFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                              bool if_exists, QueryErrorContext error_context);
template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                                 bool if_exists, QueryErrorContext error_context);
template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists, QueryErrorContext error_context);

} // namespace duckdb
