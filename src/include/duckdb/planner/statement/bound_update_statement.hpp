//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Bound equivalent to UpdateStatement
class BoundUpdateStatement : public BoundSQLStatement {
public:
	BoundUpdateStatement() : BoundSQLStatement(StatementType::UPDATE), is_index_update(false) {
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
	idx_t proj_index;
	//! Whether or not the update is an index update. Index updates are translated into insert + deletes, instead of
	//! performing an in-place update.
	bool is_index_update;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
