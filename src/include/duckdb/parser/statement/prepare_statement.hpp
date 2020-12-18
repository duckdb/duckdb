//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class PrepareStatement : public SQLStatement {
public:
	PrepareStatement() : SQLStatement(StatementType::PREPARE_STATEMENT), statement(nullptr), name("") {
	}

	unique_ptr<SQLStatement> statement;
	string name;

public:
	unique_ptr<SQLStatement> Copy() const override {
		auto result = make_unique<PrepareStatement>();
		result->statement = statement->Copy();
		result->name = name;
		return move(result);
	}
};
} // namespace duckdb
