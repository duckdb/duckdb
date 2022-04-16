//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/custom_type_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/parsed_data/create_custom_type_info.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! A custom type catalog entry
class CustomTypeCatalogEntry : public StandardEntry {
public:
	//! Create a CustomTypeCatalogEntry and initialize storage for it
	CustomTypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateCustomTypeInfo *info);

	LogicalType user_type;

public:
	//! Serialize the meta information of the CustomTypeCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CustomTypeCatalogEntry
	static unique_ptr<CreateCustomTypeInfo> Deserialize(Deserializer &source);

	string ToSQL() override;
};
} // namespace duckdb