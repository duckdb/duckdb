//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class DeleteStatement : public SQLStatement {
public:
	DeleteStatement() : SQLStatement(StatementType::DELETE) {
	}
	virtual ~DeleteStatement() {
	}

	virtual string ToString() const {
		return "Delete";
	}
	virtual unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) const {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<Expression> condition;
	unique_ptr<TableRef> table;
};
} // namespace duckdb
