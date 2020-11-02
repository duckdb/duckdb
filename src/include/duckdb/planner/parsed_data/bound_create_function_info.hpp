//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_macro_function_info.hpp"
//#include "duckdb/planner/bound_constraint.hpp"
//#include "duckdb/planner/expression.hpp"
//#include "duckdb/storage/table/persistent_segment.hpp"
//#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class CatalogEntry;

struct BoundCreateFunctionInfo {
	BoundCreateFunctionInfo(unique_ptr<CreateInfo> base) : base(move(base)) {
	}

	//! The schema to create the table in
	SchemaCatalogEntry *schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;

	BoundCreateFunctionInfo &Base() {
		return (BoundCreateFunctionInfo &)*base;
	}
};

} // namespace duckdb
