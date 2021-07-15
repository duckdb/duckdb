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
	//	//! Lock for getting a value on the sequence
	//	mutex lock;
	//	//! The amount of times the sequence has been used
	//	uint64_t usage_count;
	//	//! The sequence counter
	//	int64_t counter;
	//	//! The increment value
	//	int64_t increment;
	//	//! The minimum value of the sequence
	//	int64_t start_value;
	//	//! The minimum value of the sequence
	//	int64_t min_value;
	//	//! The maximum value of the sequence
	//	int64_t max_value;
	//	//! Whether or not the sequence cycles
	//	bool cycle;

public:
	//! Serialize the meta information of the SequenceCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	static unique_ptr<CreateEnumInfo> Deserialize(Deserializer &source);

	string ToSQL() override;
};
} // namespace duckdb
