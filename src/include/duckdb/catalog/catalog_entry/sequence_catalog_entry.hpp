//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

struct SequenceValue {
	SequenceValue() : usage_count(0), counter(-1) {
	}
	SequenceValue(uint64_t usage_count, int64_t counter) : usage_count(usage_count), counter(counter) {
	}

	uint64_t usage_count;
	int64_t counter;
};

//! A sequence catalog entry
class SequenceCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::SEQUENCE_ENTRY;
	static constexpr const char *Name = "sequence";

public:
	//! Create a real TableCatalogEntry and initialize storage for it
	SequenceCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateSequenceInfo *info);

	//! Lock for getting a value on the sequence
	mutex lock;
	//! The amount of times the sequence has been used
	uint64_t usage_count;
	//! The sequence counter
	int64_t counter;
	//! The most recently returned value
	int64_t last_value;
	//! The increment value
	int64_t increment;
	//! The minimum value of the sequence
	int64_t start_value;
	//! The minimum value of the sequence
	int64_t min_value;
	//! The maximum value of the sequence
	int64_t max_value;
	//! Whether or not the sequence cycles
	bool cycle;

public:
	//! Serialize the meta information of the SequenceCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	static unique_ptr<CreateSequenceInfo> Deserialize(Deserializer &source);

	string ToSQL() override;

	CatalogEntry *AlterOwnership(ClientContext &context, AlterInfo *info);
};
} // namespace duckdb
