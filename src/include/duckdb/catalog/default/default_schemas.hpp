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
	DefaultSchemaGenerator(Catalog &catalog);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
};

} // namespace duckdb
