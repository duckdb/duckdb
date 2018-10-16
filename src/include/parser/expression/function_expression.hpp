//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/function_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public Expression {
  public:
	FunctionExpression(std::string func_name,
	                   std::vector<std::unique_ptr<Expression>> &children);

	virtual void ResolveType() override;

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::FUNCTION;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	std::string func_name;
};
} // namespace duckdb
