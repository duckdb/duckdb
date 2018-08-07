//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/insert_statement.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"

namespace duckdb {

class InsertStatement : public SQLStatement {
  public:
	InsertStatement() : SQLStatement(StatementType::INSERT){};
	virtual ~InsertStatement() {}
	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	//! List of values to insert
	std::vector<std::unique_ptr<AbstractExpression>> values;

	//! Column names to insert into
	std::vector<std::string> columns;

	//! Table name to insert to
	std::string table;
	//! Schema name to insert to
	std::string schema;
};

} // namespace duckdb
