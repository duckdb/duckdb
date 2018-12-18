//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/transaction_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class TransactionStatement : public SQLStatement {
public:
	TransactionStatement(TransactionType type) : SQLStatement(StatementType::TRANSACTION), type(type){};
	virtual ~TransactionStatement() {
	}

	virtual string ToString() const {
		return "Transaction";
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

	TransactionType type;
};
} // namespace duckdb
