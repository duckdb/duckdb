//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/sequence_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "parser/parsed_data/create_sequence_info.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

class SchemaCatalogEntry;

//! A sequence catalog entry
class SequenceCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	SequenceCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateSequenceInfo *info)
	    : CatalogEntry(CatalogType::SEQUENCE, catalog, info->name), schema(schema), counter(info->start_value),
	      increment(info->increment), min_value(info->min_value), max_value(info->max_value), cycle(info->cycle) {
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! Lock, only used for cycling sequences
	std::mutex lock;
	//! The sequence counter
	std::atomic<int64_t> counter;
	//! The increment value
	int64_t increment;
	//! The minimum value of the sequence
	int64_t min_value;
	//! The maximum value of the sequence
	int64_t max_value;
	//! Whether or not the sequence cycles
	bool cycle;
};
} // namespace duckdb
