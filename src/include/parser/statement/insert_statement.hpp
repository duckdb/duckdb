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

#include "parser/statement/select_statement.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class InsertStatement : public SQLStatement {
  public:
	InsertStatement()
	    : SQLStatement(StatementType::INSERT), schema(DEFAULT_SCHEMA){};
	virtual ~InsertStatement() {
	}

	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) {
		v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	//! The select statement to insert from
	std::unique_ptr<SelectStatement> select_statement;

	//! List of values to insert
	std::vector<std::vector<std::unique_ptr<Expression>>> values;

	//! Column names to insert into
	std::vector<std::string> columns;

	//! Table name to insert to
	std::string table;
	//! Schema name to insert to
	std::string schema;
};

} // namespace duckdb
