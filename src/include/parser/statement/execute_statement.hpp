//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class ExecuteStatement : public SQLStatement {
public:
	ExecuteStatement() : SQLStatement(StatementType::EXECUTE){};
	string ToString() const override {
		return "Execute";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	string name;
	vector<unique_ptr<ParsedExpression>> values;
};
} // namespace duckdb
