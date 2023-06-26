//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/constraint_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_constraint_info.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

struct DataTableInfo;

//! A constraint catalog entry
class ConstraintCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::CONSTRAINT_ENTRY;
	static constexpr const char *Name = "constraint";

public:
	//! Create an ConstraintCatalogEntry and initialize storage for it
	ConstraintCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateConstraintInfo &info);

public:
	void Serialize(Serializer &serializer) const;
	static unique_ptr<CreateConstraintInfo> Deserialize(Deserializer &source, ClientContext &context);

	virtual string GetSchemaName() const = 0;
	virtual string GetTableName() const = 0;
	virtual string GetConstraintName() const = 0;
};

} // namespace duckdb
