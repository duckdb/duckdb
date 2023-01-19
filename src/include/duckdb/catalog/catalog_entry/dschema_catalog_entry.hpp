//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/dschema_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

//! A schema in the catalog
class DSchemaCatalogEntry : public SchemaCatalogEntry {
public:
	DSchemaCatalogEntry(Catalog *catalog, string name, bool is_internal);

private:
	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the copy functions
	CatalogSet copy_functions;
	//! The catalog set holding the pragma functions
	CatalogSet pragma_functions;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;
	//! The catalog set holding the sequences
	CatalogSet sequences;
	//! The catalog set holding the collations
	CatalogSet collations;
	//! The catalog set holding the types
	CatalogSet types;

public:
	CatalogEntry *AddEntryInternal(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
	                               OnCreateConflict on_conflict, DependencyList dependencies) override;
	CatalogEntry *CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) override;
	CatalogEntry *CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) override;
	void Alter(ClientContext &context, AlterInfo *info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	DropErrorType DropEntry(ClientContext &context, DropInfo *info) override;
	CatalogEntry *GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;

	void Verify(Catalog &catalog) override;

private:
	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb
