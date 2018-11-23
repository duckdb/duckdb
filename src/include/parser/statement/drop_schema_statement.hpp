//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/drop_schema_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//
#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropSchemaStatement : public SQLStatement {
  public:
	DropSchemaStatement()
	    : SQLStatement(StatementType::DROP_SCHEMA),
	      info(make_unique<DropSchemaInformation>()){};
	virtual ~DropSchemaStatement() {
	}

	virtual std::string ToString() const {
		return "DROP SCHEMA";
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

	std::unique_ptr<DropSchemaInformation> info;
};

} // namespace duckdb
