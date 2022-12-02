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
#include "duckdb/common/atomic.hpp"

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

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	SchemaCatalogEntry *schema;
	CatalogEntry *entry;

	DUCKDB_API bool Found() const {
		return entry;
	}
};

//! Return value of SimilarEntryInSchemas
struct SimilarCatalogEntry {
	//! The entry name. Empty if absent
	string name;
	//! The distance to the given name.
	idx_t distance;
	//! The schema of the entry.
	SchemaCatalogEntry *schema;

	DUCKDB_API bool Found() const {
		return !name.empty();
	}

	DUCKDB_API string GetQualifiedName() const;
};

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

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion();
	//! Trigger a modification in the catalog, increasing the catalog version and returning the previous version
	DUCKDB_API idx_t ModifyCatalog();

	//! Creates a schema in the catalog.
	DUCKDB_API CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a Enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(ClientContext &context, CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, SchemaCatalogEntry *schema,
	                                     BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                             CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                            CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                              CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                        CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(ClientContext &context, SchemaCatalogEntry *schema,
	                                        CreateSequenceInfo *info);
	//! Creates a enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(ClientContext &context, SchemaCatalogEntry *schema, CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(ClientContext &context, SchemaCatalogEntry *schema,
	                                         CreateCollationInfo *info);

	//! Drops an entry from the catalog
	DUCKDB_API void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	DUCKDB_API SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA,
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
	DUCKDB_API LogicalType GetType(ClientContext &context, const string &schema, const string &name);
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

	DUCKDB_API void Verify();

private:
	//! Reference to the database
	AttachedDatabase &db;

private:
	//! A variation of GetEntry that returns an associated schema as well.
	CatalogEntryLookup LookupEntry(ClientContext &context, CatalogType type, const string &schema, const string &name,
	                               bool if_exists = false, QueryErrorContext error_context = QueryErrorContext());

	//! Return an exception with did-you-mean suggestion.
	CatalogException CreateMissingEntryException(ClientContext &context, const string &entry_name, CatalogType type,
	                                             const vector<SchemaCatalogEntry *> &schemas,
	                                             QueryErrorContext error_context);

	//! Return the close entry name, the distance and the belonging schema.
	SimilarCatalogEntry SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
	                                          const vector<SchemaCatalogEntry *> &schemas);

	void DropSchema(ClientContext &context, DropInfo *info);
};

} // namespace duckdb
