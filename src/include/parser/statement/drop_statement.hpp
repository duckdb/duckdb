#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class DropStatement : public SQLStatement {
  public:
	DropStatement()
	    : SQLStatement(StatementType::DROP), schema(DEFAULT_SCHEMA){};
	virtual ~DropStatement() {}

	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	//! Table name to drop
	std::string table;
	//! Schema name to drop from
	std::string schema;
};

} // namespace duckdb
