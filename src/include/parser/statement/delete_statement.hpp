//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

class DeleteStatement : public SQLStatement {
public:
	DeleteStatement() : SQLStatement(StatementType::DELETE) {
	}

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
};
} // namespace duckdb
