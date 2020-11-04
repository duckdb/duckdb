//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_macro_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/macro_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class MacroFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundMacroExpression : public Expression {
public:
	BoundMacroExpression(LogicalType return_type, string name, unique_ptr<Expression> bound_expression,
	                     vector<unique_ptr<Expression>> children);
	//! The name of the expression
	string name;
	//! The bound function expression
	unique_ptr<Expression> expression;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;

public:
	bool IsFoldable() const override;
	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};

} // namespace duckdb
