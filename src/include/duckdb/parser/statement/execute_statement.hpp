//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ExecuteStatement : public SQLStatement {
public:
	ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT){};

	string name;
	vector<unique_ptr<ParsedExpression>> values;

public:
	unique_ptr<SQLStatement> Copy() const override {
		auto result = make_unique<ExecuteStatement>();
		result->name = name;
		for(auto &value : values) {
			result->values.push_back(value->Copy());
		}
		return move(result);
	}
};
} // namespace duckdb
