//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class LogicalOperator;

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<string> names;
};

} // namespace duckdb
