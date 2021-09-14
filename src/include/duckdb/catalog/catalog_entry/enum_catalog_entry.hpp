//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/enum_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_enum_info.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! A enum catalog entry
class EnumCatalogEntry : public StandardEntry {
public:
	//! Create a real EnumCatalogEntry and initialize storage for it
	 EnumCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateEnumInfo *info);

	vector<string> values_insert_order;

public:
	//! Serialize the meta information of the EnumCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateEnumInfo
	static unique_ptr<CreateEnumInfo> Deserialize(Deserializer &source);

	string ToSQL() override;
};
} // namespace duckdb
