//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_create_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/statement/bound_select_statement.hpp"

namespace duckdb {
class SchemaCatalogEntry;

//! Bound equivalent to SelectStatement
class BoundCreateTableStatement : public BoundSQLStatement {
public:
	BoundCreateTableStatement() : BoundSQLStatement(StatementType::CREATE_TABLE) {
	}

	unique_ptr<BoundCreateTableInfo> info;
	//! CREATE TABLE from QUERY
	unique_ptr<BoundSelectStatement> query;
	//! The schema to create the table in
	SchemaCatalogEntry *schema;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
