//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/statement/bound_insert_statement.hpp"

namespace duckdb {
class TableCatalogEntry;

//! Bound equivalent to CopyStatement
class BoundCopyStatement : public BoundSQLStatement {
public:
	BoundCopyStatement() : BoundSQLStatement(StatementType::COPY) {
	}

	//! The CopyInfo
	unique_ptr<CopyInfo> info;
	//! The bound insert statement (only for COPY FROM)
	unique_ptr<BoundSQLStatement> bound_insert;
	//! The bound SQL statement (only for COPY TO)
	unique_ptr<BoundQueryNode> select_statement;

	idx_t table_index;
	vector<string> names;
	vector<SQLType> sql_types;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
