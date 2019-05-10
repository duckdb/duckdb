//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function.hpp"
#include "planner/expression.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	BoundFunctionExpression(TypeId return_type, ScalarFunctionCatalogEntry *bound_function);

	// The bound function expression
	ScalarFunctionCatalogEntry *bound_function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
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
