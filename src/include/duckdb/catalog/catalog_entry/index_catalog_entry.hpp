//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"

namespace duckdb {

struct DataTableInfo;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::INDEX_ENTRY;
	static constexpr const char *Name = "index";

public:
	//! Create an IndexCatalogEntry
	IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info);

	//! The SQL of the CREATE INDEX statement
	string sql;
	//! Additional index options
	case_insensitive_map_t<Value> options;

	//! We need the initial size of the index after the CREATE INDEX statement,
	//! as it is necessary to determine the auto checkpoint threshold
	idx_t initial_index_size;
	//! The index type (ART, B+-tree, Skip-List, ...)
	string index_type;
	//! The index constraint type
	IndexConstraintType index_constraint_type;
	//! The logical column ids of the indexed table
	vector<column_t> column_ids;
	//! The set of expressions to index by
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;

public:
	//! Returns the CreateIndexInfo
	unique_ptr<CreateInfo> GetInfo() const override;
	//! Returns the original CREATE INDEX SQL
	string ToSQL() const override;

	virtual string GetSchemaName() const = 0;
	virtual string GetTableName() const = 0;

	//! Returns true, if this index is UNIQUE
	bool IsUnique();
	//! Returns true, if this index is a PRIMARY KEY
	bool IsPrimary();
};

} // namespace duckdb
