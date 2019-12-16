//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct CreateSchemaInfo : public ParseInfo {
	CreateSchemaInfo() : if_not_exists(false) {
	}

	//! Schema name to create
	string schema;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;
};

} // namespace duckdb
