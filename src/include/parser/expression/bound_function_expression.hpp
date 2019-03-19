//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/bound_function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/expression/function_expression.hpp"
namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call that has been bound to a base function
class BoundFunctionExpression : public Expression {
public:
	BoundFunctionExpression(unique_ptr<FunctionExpression> function, ScalarFunctionCatalogEntry *bound_function);

	void ResolveType() override;

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_FUNCTION;
	}

	unique_ptr<Expression> Copy() const override;

	uint64_t Hash() const override;
	bool Equals(const Expression *other) const override;

	size_t ChildCount() const override {
		return function->ChildCount();
	}
	Expression *GetChild(size_t index) const override {
		return function->GetChild(index);
	}
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override {
		return function->ReplaceChild(callback, index);
	}

	//! Serializes a BoundFunctionExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;

	string ToString() const override {
		return function->ToString();
	}

	//! The child FunctionExpression
	unique_ptr<FunctionExpression> function;
	// THe bound function expression
	ScalarFunctionCatalogEntry *bound_function;
};
} // namespace duckdb
