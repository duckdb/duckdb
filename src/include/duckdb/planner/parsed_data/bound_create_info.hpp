//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

class SchemaCatalogEntry;

struct BoundCreateInfo {
	BoundCreateInfo() {
	}
	BoundCreateInfo(unique_ptr<CreateInfo> base) : base(move(base)) {
	}
	virtual ~BoundCreateInfo() {
	}

	//! The schema to create the table in
	SchemaCatalogEntry *schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;
};

} // namespace duckdb
