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
class DCatalog : public Catalog {
public:
	explicit DCatalog(AttachedDatabase &db);
	~DCatalog();

public:
	bool IsDCatalog() override;
	void Initialize(bool load_builtin) override;

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
