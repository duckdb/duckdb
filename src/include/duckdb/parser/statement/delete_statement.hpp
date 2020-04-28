//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class DeleteStatement : public SQLStatement {
public:
	DeleteStatement() : SQLStatement(StatementType::DELETE_STATEMENT) {
	}

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
};
} // namespace duckdb
