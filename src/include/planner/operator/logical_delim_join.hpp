//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalDelimJoin represents a special "duplicate eliminated" join. This join type is only used for subquery flattening, and involves performing duplicate elimination on the LEFT side which is then pushed into the RIGHT side.
class LogicalDelimJoin : public LogicalJoin {
public:
	LogicalDelimJoin(JoinType type)
		: LogicalJoin(type, LogicalOperatorType::DELIM_JOIN) {
	}

	//! The set of columns that will be duplicate eliminated from the LHS and pushed into the RHS
	vector<unique_ptr<Expression>> duplicate_eliminated_columns;
};

} // namespace duckdb
