//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "main/result.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class DuckDBConnection;

class DuckDBPreparedStatement {
public:
	DuckDBPreparedStatement(std::unique_ptr<LogicalOperator> pplan);

	std::unique_ptr<DuckDBResult> Execute(DuckDBConnection &conn);

	void Bind(size_t param_idx, Value val);
	// void BindParam(string param_name, Value val);
	// TODO: bind primitive types?
	// TODO: variadic Execute(Value...) method?

private:
	std::unique_ptr<LogicalOperator> plan;
};

} // namespace duckdb
