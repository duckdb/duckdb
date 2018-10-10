//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/explain_statement.hpp
//
// Author: Hannes Mühleisen
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class ExplainStatement : public SQLStatement {
  public:
	ExplainStatement(std::unique_ptr<SQLStatement> stmt)
	    : SQLStatement(StatementType::EXPLAIN), stmt(move(stmt)){};
	std::unique_ptr<SQLStatement> stmt;

	virtual ~ExplainStatement() {}

	virtual std::string ToString() const { return "Explain"; }
	virtual void Accept(SQLNodeVisitor *v) {}
};

} // namespace duckdb
