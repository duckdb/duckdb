//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class LogicalOperator;
struct LogicalType;

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<string> names;
	vector<unique_ptr<ParsedExpression>> original_expressions;
};

} // namespace duckdb
