//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class ExecuteStatement : public SQLStatement {
public:
	ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT){};

	string name;
	vector<unique_ptr<ParsedExpression>> values;
};
} // namespace duckdb
