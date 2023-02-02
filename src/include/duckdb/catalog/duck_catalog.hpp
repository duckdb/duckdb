//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dcatalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//! The Catalog object represents the catalog of the database.
class DuckCatalog : public Catalog {
public:
	explicit DuckCatalog(AttachedDatabase &db);
	~DuckCatalog();

public:
	bool IsDuckCatalog() override;
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "duckdb";
	}

	DependencyManager &GetDependencyManager() {
		return *dependency_manager;
	}
	mutex &GetWriteLock() {
		return write_lock;
	}

public:
	DUCKDB_API CatalogEntry *CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info) override;
	DUCKDB_API void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) override;
	DUCKDB_API void ScanSchemas(std::function<void(CatalogEntry *)> callback);

	DUCKDB_API SchemaCatalogEntry *GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                         bool if_exists = false,
	                                         QueryErrorContext error_context = QueryErrorContext()) override;

	DUCKDB_API unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                                          unique_ptr<PhysicalOperator> plan) override;
	DUCKDB_API unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                                   unique_ptr<PhysicalOperator> plan) override;
	DUCKDB_API unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                                   unique_ptr<PhysicalOperator> plan) override;
	DUCKDB_API unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                                   unique_ptr<PhysicalOperator> plan) override;
	DUCKDB_API unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt,
	                                                       TableCatalogEntry &table,
	                                                       unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	DUCKDB_API bool InMemory() override;
	DUCKDB_API string GetDBPath() override;

private:
	DUCKDB_API void DropSchema(ClientContext &context, DropInfo *info) override;

	void Verify() override;

private:
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;
	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
};

} // namespace duckdb
