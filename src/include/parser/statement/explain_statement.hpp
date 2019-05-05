//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class ExplainStatement : public SQLStatement {
public:
	ExplainStatement(unique_ptr<SQLStatement> stmt) : SQLStatement(StatementType::EXPLAIN), stmt(move(stmt)){};

	unique_ptr<SQLStatement> stmt;
};

} // namespace duckdb
