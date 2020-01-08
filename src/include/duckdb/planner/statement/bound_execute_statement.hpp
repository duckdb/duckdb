//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"

namespace duckdb {
//! Bound equivalent to ExecuteStatement
class BoundExecuteStatement : public BoundSQLStatement {
public:
	BoundExecuteStatement() : BoundSQLStatement(StatementType::EXECUTE) {
	}

	//! The prepared statement to execute
	PreparedStatementData *prepared;
public:
	vector<string> GetNames() override {
		return prepared->names;
	}
	vector<SQLType> GetTypes() override {
		return prepared->sql_types;
	}
};
} // namespace duckdb
