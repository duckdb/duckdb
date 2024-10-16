//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_functions.hpp
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

struct DefaultMacro {
	const char *schema;
	const char *name;
	const char *parameters[8];
	DefaultNamedParameter named_parameters[8];
	const char *macro;
};

class DefaultFunctionGenerator : public DefaultGenerator {
public:
	DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

	DUCKDB_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(const DefaultMacro &default_macro);
	DUCKDB_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(array_ptr<const DefaultMacro> macro);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace duckdb
