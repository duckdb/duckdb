//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct BoundCreateIndexInfo : public BoundCreateInfo {
	BoundCreateIndexInfo(unique_ptr<CreateInfo> base) : BoundCreateInfo(move(base)) {
	}

	//! The table to index
	unique_ptr<BoundTableRef> table;
	//! Set of expressions to index by
	vector<unique_ptr<Expression>> expressions;
};

} // namespace duckdb
