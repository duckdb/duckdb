//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/sql_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
class SelectStatement;

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement : public Printable {
  public:
	SQLStatement(StatementType type) : stmt_type(type){};
	virtual ~SQLStatement() {}

	StatementType GetType() const { return stmt_type; }

	virtual void Accept(SQLNodeVisitor *) = 0;

  private:
	StatementType stmt_type;
};
} // namespace duckdb
