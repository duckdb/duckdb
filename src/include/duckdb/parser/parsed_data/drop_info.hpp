//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/drop_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

struct DropInfo {
	//! The catalog type to drop
	CatalogType type;
	//! Schema name to drop from, if any
	string schema;
	//! Element name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropInfo() : schema(INVALID_SCHEMA), if_exists(false), cascade(false) {
	}
};

} // namespace duckdb
