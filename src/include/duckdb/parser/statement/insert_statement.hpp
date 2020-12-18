//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

class InsertStatement : public SQLStatement {
public:
	InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA){};

	//! The select statement to insert from
	unique_ptr<SelectStatement> select_statement;
	//! Column names to insert into
	vector<string> columns;

	//! Table name to insert to
	string table;
	//! Schema name to insert to
	string schema;

public:
	unique_ptr<SQLStatement> Copy() const override {
		auto result = make_unique<InsertStatement>();
		result->select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(select_statement->Copy());
		result->columns = columns;
		result->table = table;
		result->schema = schema;
		return move(result);
	}
};

} // namespace duckdb
