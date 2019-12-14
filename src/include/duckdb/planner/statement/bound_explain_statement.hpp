//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_sql_statement.hpp"

namespace duckdb {
//! Bound equivalent to ExplainStatement
class BoundExplainStatement : public BoundSQLStatement {
public:
	BoundExplainStatement() : BoundSQLStatement(StatementType::EXPLAIN) {
	}

	//! The bound statement underlying the explain
	unique_ptr<BoundSQLStatement> bound_statement;

public:
	vector<string> GetNames() override {
		return {"explain_key", "explain_value"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::VARCHAR, SQLType::VARCHAR};
	}
};
} // namespace duckdb
