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
	DuckSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

private:
	enum class DuckCatalogSetType : uint8_t {
		TABLES,
		INDEXES,
		TABLE_FUNCTIONS,
		COPY_FUNCTIONS,
		PRAGMA_FUNCTIONS,
		FUNCTIONS,
		SEQUENCES,
		COLLATIONS,
		TYPES,
	};
	unordered_map<DuckCatalogSetType, unique_ptr<CatalogSet>> catalog_sets;

public:
	optional_ptr<CatalogEntry> AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
	                                    OnCreateConflict on_conflict);
	optional_ptr<CatalogEntry> AddEntryInternal(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
	                                            OnCreateConflict on_conflict, LogicalDependencyList dependencies);

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(ClientContext &context, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(ClientContext &context, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
	void ScanAll(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	void Verify(Catalog &catalog) override;

private:
	CatalogSet &Tables();
	CatalogSet &Indexes();
	CatalogSet &TableFunctions();
	CatalogSet &CopyFunctions();
	CatalogSet &PragmaFunctions();
	CatalogSet &Functions();
	CatalogSet &Sequences();
	CatalogSet &Collations();
	CatalogSet &Types();

private:
	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb
