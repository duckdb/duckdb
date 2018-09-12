//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/update_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//
#pragma once

#include <vector>

#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

class UpdateStatement : public SQLStatement {
  public:
	UpdateStatement() : SQLStatement(StatementType::UPDATE) {}
	virtual ~UpdateStatement() {}
	virtual std::string ToString() const { return "Update"; }
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	std::unique_ptr<AbstractExpression> condition;
	std::unique_ptr<TableRef> table;

	std::vector<std::string> columns;
	std::vector<std::unique_ptr<AbstractExpression>> expressions;
};
} // namespace duckdb
