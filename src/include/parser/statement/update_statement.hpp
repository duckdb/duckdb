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

#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class UpdateStatement : public SQLStatement {
  public:
	UpdateStatement() : SQLStatement(StatementType::UPDATE) {}
	virtual ~UpdateStatement() {}

	virtual std::string ToString() const { return "Update"; }
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::unique_ptr<Expression> condition;
	std::unique_ptr<TableRef> table;

	std::vector<std::string> columns;
	std::vector<std::unique_ptr<Expression>> expressions;
};
} // namespace duckdb
