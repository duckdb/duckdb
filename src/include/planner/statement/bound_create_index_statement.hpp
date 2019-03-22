//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_create_index_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "planner/bound_sql_statement.hpp"
#include "planner/bound_tableref.hpp"
#include "planner/statement/bound_select_statement.hpp"

namespace duckdb {

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
	unique_ptr<CreateIndexInformation> info;
};
} // namespace duckdb
