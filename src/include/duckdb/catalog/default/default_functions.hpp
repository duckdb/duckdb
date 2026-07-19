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
#include "duckdb/parser/parser_options.hpp"

namespace duckdb {
class SchemaCatalogEntry;

struct DefaultMacro {
	const char *schema;
	const char *name;
	const char *macro_definition; // e.g. "(param1, param2) AS expr" or "(x, sep := ',') AS expr"
};

class DefaultFunctionGenerator : public DefaultGenerator {
public:
	DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

	DUCKDB_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(const DefaultMacro &default_macro);
	//! Overload taking ParserOptions, so the caller's ParserCache is reused instead of rebuilt per macro.
	DUCKDB_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(const DefaultMacro &default_macro,
	                                                                      ParserOptions options);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const Identifier &entry_name) override;
	vector<Identifier> GetDefaultEntries() override;
};

} // namespace duckdb
