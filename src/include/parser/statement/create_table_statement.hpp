//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
public:
	CreateTableStatement() : SQLStatement(StatementType::CREATE_TABLE), info(make_unique<CreateTableInformation>()){};

	string ToString() const override {
		return "CREATE TABLE";
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

	unique_ptr<CreateTableInformation> info;
};

} // namespace duckdb
