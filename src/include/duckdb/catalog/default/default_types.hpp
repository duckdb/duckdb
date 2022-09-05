//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/catalog/default/default_generator.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DefaultTypeGenerator : public DefaultGenerator {
public:
	DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry *schema);

	SchemaCatalogEntry *schema;

public:
	DUCKDB_API static LogicalTypeId GetDefaultType(const string &name);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace duckdb
