//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/catalog_lookup_behavior.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
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
struct CreateIndexInfo;
struct CreateTypeInfo;
struct CreateTableInfo;
struct DatabaseSize;
struct MetadataBlockInfo;

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

class Binder;
class LogicalOperator;
class PhysicalOperator;
class LogicalCreateIndex;
class LogicalCreateTable;
class LogicalInsert;
class LogicalDelete;
class LogicalUpdate;
class CreateStatement;
class CatalogEntryRetriever;

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	optional_ptr<SchemaCatalogEntry> schema;
	optional_ptr<CatalogEntry> entry;
	ErrorData error;

	DUCKDB_API bool Found() const {
		return entry;
	}
};

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	explicit Catalog(AttachedDatabase &db);
	virtual ~Catalog();

public:
	//! Get the SystemCatalog from the ClientContext
	DUCKDB_API static Catalog &GetSystemCatalog(ClientContext &context);
	//! Get the SystemCatalog from the DatabaseInstance
	DUCKDB_API static Catalog &GetSystemCatalog(DatabaseInstance &db);
	//! Get the specified Catalog from the ClientContext
	DUCKDB_API static Catalog &GetCatalog(ClientContext &context, const string &catalog_name);
	//! Get the specified Catalog from the ClientContext
	DUCKDB_API static Catalog &GetCatalog(CatalogEntryRetriever &retriever, const string &catalog_name);
	//! Get the specified Catalog from the DatabaseInstance
	DUCKDB_API static Catalog &GetCatalog(DatabaseInstance &db, const string &catalog_name);
	//! Gets the specified Catalog from the database if it exists
	DUCKDB_API static optional_ptr<Catalog> GetCatalogEntry(ClientContext &context, const string &catalog_name);
	//! Gets the specified Catalog from the database if it exists
	DUCKDB_API static optional_ptr<Catalog> GetCatalogEntry(CatalogEntryRetriever &retriever,
	                                                        const string &catalog_name);
	//! Get the specific Catalog from the AttachedDatabase
	DUCKDB_API static Catalog &GetCatalog(AttachedDatabase &db);

	DUCKDB_API AttachedDatabase &GetAttached();
	DUCKDB_API const AttachedDatabase &GetAttached() const;
	DUCKDB_API DatabaseInstance &GetDatabase();

	virtual bool IsDuckCatalog() {
		return false;
	}
	virtual void Initialize(bool load_builtin) = 0;

	bool IsSystemCatalog() const;
	bool IsTemporaryCatalog() const;

	//! Returns a version number that uniquely characterizes the current catalog snapshot.
	//! If there are transaction-local changes, the version returned is >= TRANSACTION_START, o.w. it is a simple number
	//! starting at 0 that is incremented at each commit that has had catalog changes.
	//! If the catalog does not support versioning, no index is returned.
	DUCKDB_API virtual optional_idx GetCatalogVersion(ClientContext &context) {
		return {}; // don't return anything by default
	}

	//! Returns the catalog name - based on how the catalog was attached
	DUCKDB_API const string &GetName() const;
	DUCKDB_API idx_t GetOid();
	DUCKDB_API virtual string GetCatalogType() = 0;

	DUCKDB_API CatalogTransaction GetCatalogTransaction(ClientContext &context);

	//! Creates a schema in the catalog.
	DUCKDB_API virtual optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
	                                                           CreateSchemaInfo &info) = 0;
	DUCKDB_API optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);
	//! Creates a table in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
	//! Creates a table in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info);
	//! Create a table function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                                          CreateTableFunctionInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateTableFunction(ClientContext &context, CreateTableFunctionInfo &info);
	// Kept for backwards compatibility
	DUCKDB_API optional_ptr<CatalogEntry> CreateTableFunction(ClientContext &context,
	                                                          optional_ptr<CreateTableFunctionInfo> info);
	//! Create a copy function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                                         CreateCopyFunctionInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo &info);
	//! Create a pragma function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                           CreatePragmaFunctionInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo &info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateFunction(ClientContext &context, CreateFunctionInfo &info);
	//! Creates a table in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateView(ClientContext &context, CreateViewInfo &info);
	//! Creates a sequence in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateSequence(ClientContext &context, CreateSequenceInfo &info);
	//! Creates a Enum in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateType(ClientContext &context, CreateTypeInfo &info);
	//! Creates a collation in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateCollation(ClientContext &context, CreateCollationInfo &info);
	//! Creates an index in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info);
	DUCKDB_API optional_ptr<CatalogEntry> CreateIndex(ClientContext &context, CreateIndexInfo &info);

	//! Creates a table in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                  BoundCreateTableInfo &info);
	//! Create a table function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry>
	CreateTableFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema, CreateTableFunctionInfo &info);
	//! Create a copy function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                         CreateCopyFunctionInfo &info);
	//! Create a pragma function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry>
	CreatePragmaFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema, CreatePragmaFunctionInfo &info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                     CreateFunctionInfo &info);
	//! Creates a view in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                 CreateViewInfo &info);
	//! Creates a table in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                     CreateSequenceInfo &info);
	//! Creates a enum in the catalog.
	DUCKDB_API optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                 CreateTypeInfo &info);
	//! Creates a collation in the catalog
	DUCKDB_API optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, SchemaCatalogEntry &schema,
	                                                      CreateCollationInfo &info);

	//! Drops an entry from the catalog
	DUCKDB_API void DropEntry(ClientContext &context, DropInfo &info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	DUCKDB_API SchemaCatalogEntry &GetSchema(ClientContext &context, const string &name,
	                                         QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API optional_ptr<SchemaCatalogEntry> GetSchema(ClientContext &context, const string &name,
	                                                      OnEntryNotFound if_not_found,
	                                                      QueryErrorContext error_context = QueryErrorContext());
	//! Overloadable method for giving warnings on ambiguous naming id.tab due to a database and schema with name id
	DUCKDB_API virtual bool CheckAmbiguousCatalogOrSchema(ClientContext &context, const string &name) {
		return !!GetSchema(context, name, OnEntryNotFound::RETURN_NULL);
	}
	DUCKDB_API SchemaCatalogEntry &GetSchema(CatalogTransaction transaction, const string &name,
	                                         QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API virtual optional_ptr<SchemaCatalogEntry>
	GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found,
	          QueryErrorContext error_context = QueryErrorContext()) = 0;
	DUCKDB_API static SchemaCatalogEntry &GetSchema(ClientContext &context, const string &catalog_name,
	                                                const string &schema_name,
	                                                QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API static optional_ptr<SchemaCatalogEntry> GetSchema(ClientContext &context, const string &catalog_name,
	                                                             const string &schema_name,
	                                                             OnEntryNotFound if_not_found,
	                                                             QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API static optional_ptr<SchemaCatalogEntry> GetSchema(CatalogEntryRetriever &retriever,
	                                                             const string &catalog_name, const string &schema_name,
	                                                             OnEntryNotFound if_not_found,
	                                                             QueryErrorContext error_context = QueryErrorContext());
	//! Scans all the schemas in the system one-by-one, invoking the callback for each entry
	DUCKDB_API virtual void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) = 0;

	//! Gets the "schema.name" entry of the specified type, if entry does not exist behavior depends on OnEntryNotFound
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(ClientContext &context, CatalogType type, const string &schema,
	                                               const string &name, OnEntryNotFound if_not_found,
	                                               QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(CatalogEntryRetriever &retriever, CatalogType type,
	                                               const string &schema, const string &name,
	                                               OnEntryNotFound if_not_found,
	                                               QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API CatalogEntry &GetEntry(ClientContext &context, CatalogType type, const string &schema,
	                                  const string &name, QueryErrorContext error_context = QueryErrorContext());
	//! Gets the "catalog.schema.name" entry of the specified type, if entry does not exist behavior depends on
	//! OnEntryNotFound
	DUCKDB_API static optional_ptr<CatalogEntry> GetEntry(ClientContext &context, CatalogType type,
	                                                      const string &catalog, const string &schema,
	                                                      const string &name, OnEntryNotFound if_not_found,
	                                                      QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API static optional_ptr<CatalogEntry> GetEntry(CatalogEntryRetriever &retriever, CatalogType type,
	                                                      const string &catalog, const string &schema,
	                                                      const string &name, OnEntryNotFound if_not_found,
	                                                      QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API static CatalogEntry &GetEntry(ClientContext &context, CatalogType type, const string &catalog,
	                                         const string &schema, const string &name,
	                                         QueryErrorContext error_context = QueryErrorContext());

	template <class T>
	optional_ptr<T> GetEntry(ClientContext &context, const string &schema_name, const string &name,
	                         OnEntryNotFound if_not_found, QueryErrorContext error_context = QueryErrorContext()) {
		auto entry = GetEntry(context, T::Type, schema_name, name, if_not_found, error_context);
		if (!entry) {
			return nullptr;
		}
		if (entry->type != T::Type) {
			throw CatalogException(error_context, "%s is not an %s", name, T::Name);
		}
		return &entry->template Cast<T>();
	}
	template <class T>
	T &GetEntry(ClientContext &context, const string &schema_name, const string &name,
	            QueryErrorContext error_context = QueryErrorContext()) {
		auto entry = GetEntry<T>(context, schema_name, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
		return *entry;
	}

	//! Append a scalar or aggregate function to the catalog
	DUCKDB_API optional_ptr<CatalogEntry> AddFunction(ClientContext &context, CreateFunctionInfo &info);

	//! Alter an existing entry in the catalog.
	DUCKDB_API void Alter(CatalogTransaction transaction, AlterInfo &info);
	DUCKDB_API void Alter(ClientContext &context, AlterInfo &info);

	virtual unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                                       unique_ptr<PhysicalOperator> plan) = 0;
	virtual unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                                unique_ptr<PhysicalOperator> plan) = 0;
	virtual unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                                unique_ptr<PhysicalOperator> plan) = 0;
	virtual unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                                unique_ptr<PhysicalOperator> plan) = 0;
	virtual unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                                    unique_ptr<LogicalOperator> plan);
	virtual unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                                      unique_ptr<LogicalOperator> plan,
	                                                      unique_ptr<CreateIndexInfo> create_info,
	                                                      unique_ptr<AlterTableInfo> alter_info);

	virtual DatabaseSize GetDatabaseSize(ClientContext &context) = 0;
	virtual vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context);

	virtual bool InMemory() = 0;
	virtual string GetDBPath() = 0;

	//! Whether or not this catalog should search a specific type with the standard priority
	DUCKDB_API virtual CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const {
		return CatalogLookupBehavior::STANDARD;
	}

	//! Returns the default schema of the catalog
	virtual string GetDefaultSchema() const;

	//! The default table is used for `SELECT * FROM <catalog_name>;`
	//! FIXME: these should be virtual methods
	DUCKDB_API bool HasDefaultTable() const;
	DUCKDB_API void SetDefaultTable(const string &schema, const string &name);
	DUCKDB_API string GetDefaultTable() const;
	DUCKDB_API string GetDefaultTableSchema() const;

	//! Returns the dependency manager of this catalog - if the catalog has anye
	virtual optional_ptr<DependencyManager> GetDependencyManager();

public:
	template <class T>
	static optional_ptr<T> GetEntry(ClientContext &context, const string &catalog_name, const string &schema_name,
	                                const string &name, OnEntryNotFound if_not_found,
	                                QueryErrorContext error_context = QueryErrorContext()) {
		auto entry = GetEntry(context, T::Type, catalog_name, schema_name, name, if_not_found, error_context);
		if (!entry) {
			return nullptr;
		}
		if (entry->type != T::Type) {
			throw CatalogException(error_context, "%s is not an %s", name, T::Name);
		}
		return &entry->template Cast<T>();
	}
	template <class T>
	static T &GetEntry(ClientContext &context, const string &catalog_name, const string &schema_name,
	                   const string &name, QueryErrorContext error_context = QueryErrorContext()) {
		auto entry =
		    GetEntry<T>(context, catalog_name, schema_name, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
		return *entry;
	}

	DUCKDB_API vector<reference<SchemaCatalogEntry>> GetSchemas(ClientContext &context);
	DUCKDB_API static vector<reference<SchemaCatalogEntry>> GetSchemas(ClientContext &context,
	                                                                   const string &catalog_name);
	DUCKDB_API static vector<reference<SchemaCatalogEntry>> GetSchemas(CatalogEntryRetriever &retriever,
	                                                                   const string &catalog_name);
	DUCKDB_API static vector<reference<SchemaCatalogEntry>> GetAllSchemas(ClientContext &context);

	virtual void Verify();

	static CatalogException UnrecognizedConfigurationError(ClientContext &context, const string &name);

	//! Autoload the extension required for `configuration_name` or throw a CatalogException
	static void AutoloadExtensionByConfigName(ClientContext &context, const string &configuration_name);
	//! Autoload the extension required for `function_name` or throw a CatalogException
	static bool AutoLoadExtensionByCatalogEntry(DatabaseInstance &db, CatalogType type, const string &entry_name);
	DUCKDB_API static bool TryAutoLoad(ClientContext &context, const string &extension_name) noexcept;

protected:
	//! Reference to the database
	AttachedDatabase &db;

	//! (optionally) a default table to query for `SELECT * FROM <catalog_name>;`
	string default_table;
	string default_table_schema;

public:
	//! Lookup an entry using TryLookupEntry, throws if entry not found and if_not_found == THROW_EXCEPTION
	CatalogEntryLookup LookupEntry(CatalogEntryRetriever &retriever, CatalogType type, const string &schema,
	                               const string &name, OnEntryNotFound if_not_found,
	                               QueryErrorContext error_context = QueryErrorContext());

private:
	//! Lookup an entry in the schema, returning a lookup with the entry and schema if they exist
	CatalogEntryLookup TryLookupEntryInternal(CatalogTransaction transaction, CatalogType type, const string &schema,
	                                          const string &name);
	//! Calls LookupEntryInternal on the schema, trying other schemas if the schema is invalid. Sets
	//! CatalogEntryLookup->error depending on if_not_found when no entry is found
	CatalogEntryLookup TryLookupEntry(CatalogEntryRetriever &retriever, CatalogType type, const string &schema,
	                                  const string &name, OnEntryNotFound if_not_found,
	                                  QueryErrorContext error_context = QueryErrorContext());
	static CatalogEntryLookup TryLookupEntry(CatalogEntryRetriever &retriever, vector<CatalogLookup> &lookups,
	                                         CatalogType type, const string &name, OnEntryNotFound if_not_found,
	                                         QueryErrorContext error_context = QueryErrorContext());
	static CatalogEntryLookup TryLookupEntry(CatalogEntryRetriever &retriever, CatalogType type, const string &catalog,
	                                         const string &schema, const string &name, OnEntryNotFound if_not_found,
	                                         QueryErrorContext error_context);

	//! Looks for a Catalog with a DefaultTable that matches the lookup
	static CatalogEntryLookup TryLookupDefaultTable(CatalogEntryRetriever &retriever, CatalogType type,
	                                                const string &catalog, const string &schema, const string &name,
	                                                OnEntryNotFound if_not_found, QueryErrorContext error_context);

	//! Return an exception with did-you-mean suggestion.
	static CatalogException CreateMissingEntryException(CatalogEntryRetriever &retriever, const string &entry_name,
	                                                    CatalogType type,
	                                                    const reference_set_t<SchemaCatalogEntry> &schemas,
	                                                    QueryErrorContext error_context);

	//! Return the close entry name, the distance and the belonging schema.
	static vector<SimilarCatalogEntry> SimilarEntriesInSchemas(ClientContext &context, const string &entry_name,
	                                                           CatalogType type,
	                                                           const reference_set_t<SchemaCatalogEntry> &schemas);

	virtual void DropSchema(ClientContext &context, DropInfo &info) = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
