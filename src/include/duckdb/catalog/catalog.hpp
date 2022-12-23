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
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/atomic.hpp"

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
struct CreateTypeInfo;
struct CreateTableInfo;

class AttachedDatabase;
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
class CatalogSet;
class DatabaseInstance;
class DependencyManager;

struct CatalogLookup;
struct CatalogEntryLookup;
struct SimilarCatalogEntry;

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	explicit Catalog(AttachedDatabase &db);
	~Catalog();

	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;

public:
	//! Get the SystemCatalog from the ClientContext
	DUCKDB_API static Catalog &GetSystemCatalog(ClientContext &context);
	//! Get the SystemCatalog from the DatabaseInstance
	DUCKDB_API static Catalog &GetSystemCatalog(DatabaseInstance &db);
	//! Get the specified Catalog from the ClientContext
	DUCKDB_API static Catalog &GetCatalog(ClientContext &context, const string &catalog_name);
	//! Get the specified Catalog from the DatabaseInstance
	DUCKDB_API static Catalog &GetCatalog(DatabaseInstance &db, const string &catalog_name);
	//! Get the specific Catalog from the AttachedDatabase
	DUCKDB_API static Catalog &GetCatalog(AttachedDatabase &db);

	DUCKDB_API DependencyManager &GetDependencyManager() {
		return *dependency_manager;
	}
	DUCKDB_API AttachedDatabase &GetAttached();
	DUCKDB_API DatabaseInstance &GetDatabase();

	void Initialize(bool load_builtin);

	bool IsSystemCatalog() const;
	bool IsTemporaryCatalog() const;

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion();
	//! Trigger a modification in the catalog, increasing the catalog version and returning the previous version
	DUCKDB_API idx_t ModifyCatalog();

	//! Returns the catalog name - based on how the catalog was attached
	DUCKDB_API const string &GetName();
	DUCKDB_API idx_t GetOid();

	DUCKDB_API CatalogTransaction GetCatalogTransaction(ClientContext &context);

	//! Creates a schema in the catalog.
	DUCKDB_API CatalogEntry *CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info);
	DUCKDB_API CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info);
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo *info);
	DUCKDB_API CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo *info);
	DUCKDB_API CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo *info);
	DUCKDB_API CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info);
	DUCKDB_API CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(CatalogTransaction transaction, CreateViewInfo *info);
	DUCKDB_API CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(CatalogTransaction transaction, CreateSequenceInfo *info);
	DUCKDB_API CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a Enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(CatalogTransaction transaction, CreateTypeInfo *info);
	DUCKDB_API CatalogEntry *CreateType(ClientContext &context, CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(CatalogTransaction transaction, CreateCollationInfo *info);
	DUCKDB_API CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                     BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                             CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                            CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                              CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                        CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                    CreateViewInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                        CreateSequenceInfo *info);
	//! Creates a enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                    CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(CatalogTransaction transaction, SchemaCatalogEntry *schema,
	                                         CreateCollationInfo *info);

	//! Drops an entry from the catalog
	DUCKDB_API void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	DUCKDB_API SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA,
	                                         bool if_exists = false,
	                                         QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API SchemaCatalogEntry *GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                         bool if_exists = false,
	                                         QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API static SchemaCatalogEntry *GetSchema(ClientContext &context, const string &catalog_name,
	                                                const string &schema_name, bool if_exists = false,
	                                                QueryErrorContext error_context = QueryErrorContext());
	//! Scans all the schemas in the system one-by-one, invoking the callback for each entry
	DUCKDB_API void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback);
	//! Gets the "schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not
	//! exist, otherwise an exception is thrown
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, CatalogType type, const string &schema,
	                                  const string &name, bool if_exists = false,
	                                  QueryErrorContext error_context = QueryErrorContext());
	//! Gets the "catalog.schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not
	//! exist, otherwise an exception is thrown
	DUCKDB_API static CatalogEntry *GetEntry(ClientContext &context, CatalogType type, const string &catalog,
	                                         const string &schema, const string &name, bool if_exists = false,
	                                         QueryErrorContext error_context = QueryErrorContext());

	//! Gets the "schema.name" entry without a specified type, if entry does not exist an exception is thrown
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, const string &schema, const string &name);

	//! Fetches a logical type from the catalog
	DUCKDB_API static LogicalType GetType(ClientContext &context, const string &catalog_name, const string &schema,
	                                      const string &name);

	template <class T>
	T *GetEntry(ClientContext &context, const string &schema_name, const string &name, bool if_exists = false,
	            QueryErrorContext error_context = QueryErrorContext()) {
		auto entry = GetEntry(context, T::Type, schema_name, name, if_exists, error_context);
		if (entry && entry->type != T::Type) {
			throw CatalogException(error_context.FormatError("%s is not an %s", name, T::Name));
		}
		return (T *)entry;
	}

	//! Append a scalar or aggregate function to the catalog
	DUCKDB_API CatalogEntry *AddFunction(ClientContext &context, CreateFunctionInfo *info);

	//! Alter an existing entry in the catalog.
	DUCKDB_API void Alter(ClientContext &context, AlterInfo *info);

public:
	template <class T>
	static T *GetEntry(ClientContext &context, const string &catalog_name, const string &schema_name,
	                   const string &name, bool if_exists = false,
	                   QueryErrorContext error_context = QueryErrorContext()) {
		auto entry = GetEntry(context, T::Type, catalog_name, schema_name, name, if_exists, error_context);
		if (entry && entry->type != T::Type) {
			throw CatalogException(error_context.FormatError("%s is not an %s", name, T::Name));
		}
		return (T *)entry;
	}

	DUCKDB_API static vector<SchemaCatalogEntry *> GetSchemas(ClientContext &context, const string &catalog_name);
	DUCKDB_API static vector<SchemaCatalogEntry *> GetAllSchemas(ClientContext &context);

	DUCKDB_API void Verify();

private:
	//! Reference to the database
	AttachedDatabase &db;

private:
	CatalogEntryLookup LookupEntryInternal(CatalogTransaction transaction, CatalogType type, const string &schema,
	                                       const string &name);
	CatalogEntryLookup LookupEntry(ClientContext &context, CatalogType type, const string &schema, const string &name,
	                               bool if_exists = false, QueryErrorContext error_context = QueryErrorContext());
	static CatalogEntryLookup LookupEntry(ClientContext &context, vector<CatalogLookup> &lookups, CatalogType type,
	                                      const string &name, bool if_exists = false,
	                                      QueryErrorContext error_context = QueryErrorContext());

	//! Return an exception with did-you-mean suggestion.
	static CatalogException CreateMissingEntryException(ClientContext &context, const string &entry_name,
	                                                    CatalogType type,
	                                                    const unordered_set<SchemaCatalogEntry *> &schemas,
	                                                    QueryErrorContext error_context);

	//! Return the close entry name, the distance and the belonging schema.
	static SimilarCatalogEntry SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
	                                                 const unordered_set<SchemaCatalogEntry *> &schemas);

	void DropSchema(ClientContext &context, DropInfo *info);
};

} // namespace duckdb
