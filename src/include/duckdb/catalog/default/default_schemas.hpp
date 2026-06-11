//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_schemas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/default/default_generator.hpp"

namespace duckdb {

class DefaultSchemaGenerator : public DefaultGenerator {
public:
	explicit DefaultSchemaGenerator(Catalog &catalog);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction transaction, const Identifier &entry_name) override;
	vector<Identifier> GetDefaultEntries() override;
	static bool IsDefaultSchema(const Identifier &input_schema);
};

} // namespace duckdb
