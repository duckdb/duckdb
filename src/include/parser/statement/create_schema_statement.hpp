//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/create_schema_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateSchemaStatement : public SQLStatement {
  public:
	CreateSchemaStatement()
	    : SQLStatement(StatementType::CREATE_SCHEMA),
	      info(make_unique<CreateSchemaInformation>()){};
	virtual ~CreateSchemaStatement() {}

	virtual std::string ToString() const { return "CREATE SCHEMA"; }
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::unique_ptr<CreateSchemaInformation> info;
};

} // namespace duckdb
