//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DeleteStatement : public SQLStatement {
public:
	DeleteStatement() : SQLStatement(StatementType::DELETE) {
	}

	string ToString() const override {
		return "Delete";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
};
} // namespace duckdb
