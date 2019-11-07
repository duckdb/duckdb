//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct CreateSchemaInfo {
	//! Schema name to create
	string schema;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateSchemaInfo() : if_not_exists(false) {
	}
};

} // namespace duckdb
