//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/transaction_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//
#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class TransactionStatement : public SQLStatement {
  public:
	TransactionStatement(TransactionType type)
	    : SQLStatement(StatementType::TRANSACTION), type(type){};
	virtual ~TransactionStatement() {}
	virtual std::string ToString() const { return "Transaction"; }
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	TransactionType type;
};
} // namespace duckdb
