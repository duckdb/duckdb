//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class UpdateStatement : public SQLStatement {
public:
	UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT) {
	}

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
	unique_ptr<TableRef> from_table;
	vector<string> columns;
	vector<unique_ptr<ParsedExpression>> expressions;

public:
	unique_ptr<SQLStatement> Copy() const override {
		auto result = make_unique<UpdateStatement>();
		result->condition = condition->Copy();
		result->table = table->Copy();
		result->from_table = from_table->Copy();
		result->columns = columns;
		for(auto &expr : expressions) {
			result->expressions.push_back(expr->Copy());
		}
		return move(result);
	}
};
} // namespace duckdb
