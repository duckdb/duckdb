//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/delete_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//
#pragma once

#include <vector>

#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class DeleteStatement : public SQLStatement {
  public:
	DeleteStatement() : SQLStatement(StatementType::DELETE) {
	}
	virtual ~DeleteStatement() {
	}

	virtual std::string ToString() const {
		return "Delete";
	}
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::unique_ptr<Expression> condition;
	std::unique_ptr<TableRef> table;
};
} // namespace duckdb
