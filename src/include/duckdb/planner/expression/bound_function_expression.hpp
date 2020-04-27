//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	BoundFunctionExpression(TypeId return_type, ScalarFunction bound_function, bool is_operator = false);

	// The bound function expression
	ScalarFunction function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! Argument types
	vector<SQLType> arguments;
	//! The return type
	SQLType sql_return_type;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;

public:
	bool IsFoldable() const override;
	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
