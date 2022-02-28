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
	InsertStatement();

	//! The select statement to insert from
	unique_ptr<SelectStatement> select_statement;
	//! Column names to insert into
	vector<string> columns;

	//! Table name to insert to
	string table;
	//! Schema name to insert to
	string schema;

	//! create a returning statement (interpreted as a select statement).
	unique_ptr<SelectStatement> returning_statement;
	//! keep track of optional returningList if it's a RETURNING statement
	vector<unique_ptr<ParsedExpression>> returning_list;

protected:
	InsertStatement(const InsertStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
