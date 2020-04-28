//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class ExplainStatement : public SQLStatement {
public:
	ExplainStatement(unique_ptr<SQLStatement> stmt) : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(move(stmt)){};

	unique_ptr<SQLStatement> stmt;
};

} // namespace duckdb
