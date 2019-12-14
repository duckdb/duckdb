//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_create_index_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/statement/bound_select_statement.hpp"

namespace duckdb {
class BoundColumnRefExpression;

//! Bound equivalent to CreateIndexStatement
class BoundCreateIndexStatement : public BoundSQLStatement {
public:
	BoundCreateIndexStatement() : BoundSQLStatement(StatementType::CREATE_INDEX) {
	}

	//! The table to index
	unique_ptr<BoundTableRef> table;
	//! Set of expressions to index by
	vector<unique_ptr<Expression>> expressions;
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
