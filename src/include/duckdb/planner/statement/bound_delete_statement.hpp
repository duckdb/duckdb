//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Bound equivalent to DeleteStatement
class BoundDeleteStatement : public BoundSQLStatement {
public:
	BoundDeleteStatement() : BoundSQLStatement(StatementType::DELETE) {
	}

	unique_ptr<Expression> condition;
	unique_ptr<BoundTableRef> table;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
