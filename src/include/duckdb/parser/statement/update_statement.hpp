//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class UpdateStatement : public SQLStatement {
public:
	UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT) {
	}

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;

	vector<string> columns;
	vector<unique_ptr<ParsedExpression>> expressions;
};
} // namespace duckdb
