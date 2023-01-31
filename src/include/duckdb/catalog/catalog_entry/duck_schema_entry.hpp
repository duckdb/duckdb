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
class DuckSchemaEntry : public SchemaCatalogEntry {
public:
	DuckSchemaEntry(Catalog *catalog, string name, bool is_internal);

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
	CatalogEntry *AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
	                       OnCreateConflict on_conflict);
	CatalogEntry *AddEntryInternal(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
	                               OnCreateConflict on_conflict, DependencyList dependencies);

	CatalogEntry *CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) override;
	CatalogEntry *CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) override;
	CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) override;
	CatalogEntry *CreateView(CatalogTransaction transaction, CreateViewInfo *info) override;
	CatalogEntry *CreateSequence(CatalogTransaction transaction, CreateSequenceInfo *info) override;
	CatalogEntry *CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo *info) override;
	CatalogEntry *CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo *info) override;
	CatalogEntry *CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo *info) override;
	CatalogEntry *CreateCollation(CatalogTransaction transaction, CreateCollationInfo *info) override;
	CatalogEntry *CreateType(CatalogTransaction transaction, CreateTypeInfo *info) override;
	void Alter(ClientContext &context, AlterInfo *info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo *info) override;
	CatalogEntry *GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;

	void Verify(Catalog &catalog) override;

private:
	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb
