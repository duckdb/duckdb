//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"

namespace duckdb {

//! Bound equivalent to SelectStatement
class BoundSelectStatement : public BoundSQLStatement {
public:
	BoundSelectStatement() : BoundSQLStatement(StatementType::SELECT) {
	}

	//! The main query node
	unique_ptr<BoundQueryNode> node;

public:
	vector<string> GetNames() override {
		return node->names;
	}
	vector<SQLType> GetTypes() override {
		return node->types;
	}
};
} // namespace duckdb
