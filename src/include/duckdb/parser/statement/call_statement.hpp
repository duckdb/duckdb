//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/call_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class CallStatement : public SQLStatement {
public:
	CallStatement() : SQLStatement(StatementType::CALL_STATEMENT) {};

	unique_ptr<ParsedExpression> function;
};
} // namespace duckdb
