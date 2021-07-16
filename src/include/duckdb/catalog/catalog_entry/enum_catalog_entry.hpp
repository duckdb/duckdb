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
	//! For now I'm duplicating the data for fast access
	unordered_map<string, idx_t> values;
	vector<string> string_values;
public:
	//! Serialize the meta information of the SequenceCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	static unique_ptr<CreateEnumInfo> Deserialize(Deserializer &source);

	string ToSQL() override;
};
} // namespace duckdb
