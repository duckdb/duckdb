//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public Expression {
public:
	FunctionExpression(string schema_name, string function_name, vector<unique_ptr<Expression>> &children);
	FunctionExpression(string function_name, vector<unique_ptr<Expression>> &children)
	    : FunctionExpression(DEFAULT_SCHEMA, function_name, children) {
	}

	void ResolveType() override;

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::FUNCTION;
	}

	unique_ptr<Expression> Copy() override;

	uint64_t Hash() const override;
	bool Equals(const Expression *other) const override;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	//! Serializes a FunctionExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an FunctionExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	string ToString() const override;

	//! Schema of the function
	string schema;
	//! Function name
	string function_name;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
};
} // namespace duckdb
