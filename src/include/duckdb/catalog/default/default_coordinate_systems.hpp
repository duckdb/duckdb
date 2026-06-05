//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_coordinate_systems.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/common/array_ptr.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DefaultCoordinateSystemGenerator : public DefaultGenerator {
public:
	DefaultCoordinateSystemGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace duckdb
