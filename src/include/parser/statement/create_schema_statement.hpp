//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_schema_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateSchemaStatement : public SQLStatement {
public:
	CreateSchemaStatement()
	    : SQLStatement(StatementType::CREATE_SCHEMA), info(make_unique<CreateSchemaInformation>()){};

	string ToString() const override {
		return "CREATE SCHEMA";
	}
	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<CreateSchemaInformation> info;
};

} // namespace duckdb
