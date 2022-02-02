//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<string> names;
	vector<unique_ptr<BoundTableRef>> unreferenced;
};

} // namespace duckdb
