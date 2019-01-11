//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_schema_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropSchemaStatement : public SQLStatement {
public:
	DropSchemaStatement() : SQLStatement(StatementType::DROP_SCHEMA), info(make_unique<DropSchemaInformation>()){};

	string ToString() const override {
		return "DROP SCHEMA";
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

	unique_ptr<DropSchemaInformation> info;
};

} // namespace duckdb
