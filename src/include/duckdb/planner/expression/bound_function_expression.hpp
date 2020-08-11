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
	BoundFunctionExpression(TypeId return_type, SQLType sql_type, ScalarFunction bound_function, bool is_operator = false);
	BoundFunctionExpression(TypeId return_type, SQLType sql_type, ScalarFunction bound_function, vector<SQLType> arguments, bool is_operator = false);
	BoundFunctionExpression(SQLType return_type, ScalarFunction bound_function, bool is_operator = false) :
		BoundFunctionExpression(GetInternalType(return_type), move(return_type), move(bound_function), is_operator) {}
	BoundFunctionExpression(SQLType return_type, ScalarFunction bound_function, vector<SQLType> arguments, bool is_operator = false) :
		BoundFunctionExpression(GetInternalType(return_type), move(return_type), move(bound_function), move(arguments), is_operator) {}


	// The bound function expression
	ScalarFunction function;
	//! List of child-expressions of the function
	vector<unique_ptr<Expression>> children;
	//! Argument types of the function. This is separate from the actual function in case of e.g. varargs functions, where the function definition only contains "varargs", this will contain the number of arguments for this invocation.
	vector<SQLType> arguments;
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
