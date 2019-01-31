//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "execution/physical_operator.hpp"
#include "main/result.hpp"

#include <map>

namespace duckdb {

class DuckDBConnection;

class DuckDBPreparedStatement {
public:
	DuckDBPreparedStatement(std::unique_ptr<PhysicalOperator> pplan, vector<string> names);

	std::unique_ptr<DuckDBResult> Execute(DuckDBConnection &conn);

	DuckDBPreparedStatement *Bind(size_t param_idx, Value val);
	// TODO: bind primitive types?
	// TODO: variadic Execute(Value...) method?
	std::unordered_map<size_t, ParameterExpression *> parameter_expression_map;

private:
	std::unique_ptr<PhysicalOperator> plan;
	vector<string> names;
};

} // namespace duckdb
