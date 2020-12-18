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

public:
	unique_ptr<SQLStatement> Copy() const override {
		auto result = make_unique<ExplainStatement>(stmt->Copy());
		return move(result);
	}
};

} // namespace duckdb
