//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/statement/bound_insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_sql_statement.hpp"
#include "planner/statement/bound_select_statement.hpp"

namespace duckdb {
class TableCatalogEntry;

//! Bound equivalent to InsertStatement
class BoundInsertStatement : public BoundSQLStatement {
public:
	BoundInsertStatement() : BoundSQLStatement(StatementType::INSERT) {
	}

	//! The table entry to insert into
	TableCatalogEntry *table;
	//! The bound select statement (if any)
	unique_ptr<BoundSelectStatement> select_statement;
	//! The bound expressions to insert (if any)
	vector<vector<unique_ptr<Expression>>> values;
	//! The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
	vector<index_t> column_index_map;
	//! The expected types for the INSERT statement (obtained from the column types)
	vector<SQLType> expected_types;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType(SQLTypeId::BIGINT)};
	}
};
} // namespace duckdb
