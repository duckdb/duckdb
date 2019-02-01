//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class PrepareStatement : public SQLStatement {
public:
	PrepareStatement() : SQLStatement(StatementType::PREPARE) {
	}
	string ToString() const override {
		return "Prepare";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<SQLStatement> statement;
	string name;
};
} // namespace duckdb
