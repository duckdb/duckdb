//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

struct DropInfo : public ParseInfo {
	DropInfo() : catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), if_exists(false), cascade(false) {
	}

	//! The catalog type to drop
	CatalogType type;
	//! Catalog name to drop from, if any
	string catalog;
	//! Schema name to drop from, if any
	string schema;
	//! Element name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;
	//! Allow dropping of internal system entries
	bool allow_drop_internal = false;

public:
	unique_ptr<DropInfo> Copy() const {
		auto result = make_unique<DropInfo>();
		result->type = type;
		result->catalog = catalog;
		result->schema = schema;
		result->name = name;
		result->if_exists = if_exists;
		result->cascade = cascade;
		result->allow_drop_internal = allow_drop_internal;
		return result;
	}
};

} // namespace duckdb
