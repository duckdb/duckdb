//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_sql_statement.hpp"
#include "planner/bound_tableref.hpp"

namespace duckdb {

//! Bound equivalent to UpdateStatement
class BoundUpdateStatement : public BoundSQLStatement {
public:
	BoundUpdateStatement() : BoundSQLStatement(StatementType::UPDATE) {
	}

	//! The condition by which to update
	unique_ptr<Expression> condition;
	//! The table to update
	unique_ptr<BoundTableRef> table;
	//! The column ids to update
	vector<column_t> column_ids;
	//! The expressions to update by
	vector<unique_ptr<Expression>> expressions;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;
	//! The projection index
	index_t proj_index;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType(SQLTypeId::BIGINT)};
	}
};
} // namespace duckdb
