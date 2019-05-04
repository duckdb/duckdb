//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_query_node.hpp"
#include "planner/bound_sql_statement.hpp"

namespace duckdb {
class TableCatalogEntry;

//! Bound equivalent to CopyStatement
class BoundCopyStatement : public BoundSQLStatement {
public:
	BoundCopyStatement() : BoundSQLStatement(StatementType::COPY) {
	}

	//! The CopyInfo
	unique_ptr<CopyInfo> info;
	//! The destination table (only for COPY from file -> database)
	TableCatalogEntry *table;
	// The bound SQL statement (only for COPY from database -> file)
	unique_ptr<BoundQueryNode> select_statement;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType(SQLTypeId::BIGINT)};
	}
};
} // namespace duckdb
