//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_sql_statement.hpp"

namespace duckdb {
class PreparedStatementCatalogEntry;

//! Bound equivalent to ExecuteStatement
class BoundExecuteStatement : public BoundSQLStatement {
public:
	BoundExecuteStatement() : BoundSQLStatement(StatementType::EXECUTE) {
	}

	//! The substitution values
	vector<Value> values;
	//! The prepared statement to execute
	PreparedStatementCatalogEntry *prep;
};
} // namespace duckdb
