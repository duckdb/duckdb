//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "planner/expression.hpp"
#include "common/enums/operator_type.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	BoundFunctionExpression(TypeId return_type, ScalarFunction bound_function, OperatorType op_type);

	// The bound function expression
	ScalarFunction function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! The built-in operator type of the function (if any)
	OperatorType op_type;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;

public:
	bool IsFoldable() const override;
	string ToString() const override;

	uint64_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
