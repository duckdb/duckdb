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
	ExecuteStatement();

	string name;
	vector<unique_ptr<ParsedExpression>> values;

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
