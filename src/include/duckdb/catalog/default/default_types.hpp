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
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {
class SchemaCatalogEntry;

struct DefaultType {
	const char *name;
	LogicalType type;
	bind_logical_type_function_t bind_function;
};

class DefaultTypeGenerator : public DefaultGenerator {
public:
	DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	DUCKDB_API static LogicalType GetDefaultType(const string &name);
	DUCKDB_API static LogicalType TryDefaultBind(const string &name, const vector<pair<string, Value>> &params);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace duckdb
