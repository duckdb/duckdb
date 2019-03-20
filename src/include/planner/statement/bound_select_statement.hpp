//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_sql_statement.hpp"

namespace duckdb {

//! Bound equivalent to SelectStatement
class BoundSelectStatement : public BoundSQLStatement {
public:
	BoundSelectStatement() : BoundSQLStatement(StatementType::SELECT) {
	}

	//! The main query node
	unique_ptr<BoundQueryNode> node;
};
} // namespace duckdb
