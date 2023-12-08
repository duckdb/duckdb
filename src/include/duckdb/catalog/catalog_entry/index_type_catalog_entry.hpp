//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_type_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_type_info.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

typedef unique_ptr<Index> (*index_create_function_t)(const string &name,
                                                     const IndexConstraintType index_constraint_type,
                                                     const vector<column_t> &column_ids,
                                                     TableIOManager &table_io_manager,
                                                     const vector<unique_ptr<Expression>> &unbound_expressions,
                                                     AttachedDatabase &db, const IndexStorageInfo &storage_info);

//! An catalog entry for an index "type"
class IndexTypeCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::INDEX_TYPE_ENTRY;
	static constexpr const char *Name = "index type";

public:
	//! Create an IndexCatalogEntry
	IndexTypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexTypeInfo &info);

public:
	// Callbacks
	index_create_function_t create_instance;
};

} // namespace duckdb
