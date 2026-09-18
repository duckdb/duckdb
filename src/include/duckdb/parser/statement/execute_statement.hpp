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
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {

class ExecuteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXECUTE_STATEMENT;

public:
	ExecuteStatement();

	Identifier name;
	identifier_map_t<unique_ptr<ParsedExpression>> named_values;
	//! Parameter values that are already typed - set when executing a prepared statement through the C/C++ API
	//! instead of through SQL, where the values would have to be bound as literals first
	identifier_map_t<BoundParameterData> bound_values;

protected:
	ExecuteStatement(const ExecuteStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace duckdb
