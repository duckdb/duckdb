//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/passthrough_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

//! What a passthrough catalog type is made of: its storage type name and the table function that executes the SQL
//! forwarded to catalogs of that type.
struct PassthroughStorageInfo : public StorageExtensionInfo {
	PassthroughStorageInfo(string type_name_p, QualifiedName query_function_p)
	    : type_name(std::move(type_name_p)), query_function(std::move(query_function_p)) {
	}

	string type_name;
	QualifiedName query_function;
};

//! A catalog without entries of its own. It exists to be a CONNECT target: statements forwarded to it become calls
//! of `query_function(path, sql, option := value, ...)`, where the named arguments are the ATTACH options. This is
//! how an extension built against the C API, which cannot implement a Catalog, still provides `CONNECT 'type:...'`.
class PassthroughCatalog : public Catalog {
public:
	PassthroughCatalog(AttachedDatabase &db, string type_name, QualifiedName query_function, string path,
	                   unordered_map<string, Value> options);
	~PassthroughCatalog() override;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override;

private:
	string type_name;
	QualifiedName query_function;
	string path;
	unordered_map<string, Value> options;
};

//! A passthrough catalog has nothing to make transactional; this manager only hands out transaction objects.
class PassthroughTransactionManager : public TransactionManager {
public:
	explicit PassthroughTransactionManager(AttachedDatabase &db);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

//! The storage extension behind a passthrough catalog type. Register it under the type name with
//! `StorageExtension::Register`.
class PassthroughStorageExtension : public StorageExtension {
public:
	PassthroughStorageExtension(string type_name, QualifiedName query_function);
};

} // namespace duckdb
