//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! A macro function in the catalog
class MacroCatalogEntry : public FunctionEntry {
public:
	MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);

	//! The macro function
	unique_ptr<MacroFunction> function;

public:
	virtual unique_ptr<CreateMacroInfo> GetInfoForSerialization() const;
	//! Serialize the meta information
	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<CreateMacroInfo> Deserialize(Deserializer &main_source, ClientContext &context);

	string ToSQL() const override {
		return function->ToSQL(schema.name, name);
	}
};

} // namespace duckdb
