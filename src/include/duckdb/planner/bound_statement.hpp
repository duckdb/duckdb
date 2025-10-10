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
#include "duckdb/common/enums/set_operation_type.hpp"

namespace duckdb {

class LogicalOperator;
struct LogicalType;
struct BoundStatement;

struct ExtraBoundInfo {
	vector<unique_ptr<ParsedExpression>> original_expressions;
};

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<string> names;
	ExtraBoundInfo extra_info;
};

} // namespace duckdb
