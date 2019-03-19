//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class PrepareStatement : public SQLStatement {
public:
	PrepareStatement() : SQLStatement(StatementType::PREPARE), statement(nullptr), name("") {
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
