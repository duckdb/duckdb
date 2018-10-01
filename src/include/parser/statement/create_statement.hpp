//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/create_statement.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/column_definition.hpp"

#include "parser/sql_statement.hpp"

#include "parser/constraint.hpp"
#include "parser/expression.hpp"

namespace duckdb {

class CreateStatement : public SQLStatement {
  public:
	CreateStatement()
	    : SQLStatement(StatementType::CREATE), schema(DEFAULT_SCHEMA){};
	virtual ~CreateStatement() {}

	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	//! Table name to insert to
	std::string table;
	//! Schema name to insert to
	std::string schema;
	//! List of columns of the table
	std::vector<ColumnDefinition> columns;
	//! List of constraints on the table
	std::vector<std::unique_ptr<Constraint>> constraints;
};

} // namespace duckdb
