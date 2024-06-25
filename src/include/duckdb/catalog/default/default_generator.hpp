//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class ClientContext;

class DefaultGenerator {
public:
	explicit DefaultGenerator(Catalog &catalog);
	virtual ~DefaultGenerator();

	Catalog &catalog;
	atomic<bool> created_all_entries;

public:
	//! Creates a default entry with the specified name, or returns nullptr if no such entry can be generated
	virtual unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name);
	virtual unique_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction transaction, const string &entry_name);
	//! Get a list of all default entries in the generator
	virtual vector<string> GetDefaultEntries() = 0;
};

} // namespace duckdb
