//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/operator_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public ParsedExpression {
public:
	OperatorExpression(ExpressionType type,
	                   unique_ptr<ParsedExpression> left = nullptr,
	                   unique_ptr<ParsedExpression> right = nullptr);

	vector<unique_ptr<ParsedExpression>> children;
public:
	string ToString() const override;

	bool Equals(const ParsedExpression *other) const override;

	unique_ptr<ParsedExpression> Copy() override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);

	size_t ChildCount() const override;
	ParsedExpression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
	                  size_t index) override;
};
} // namespace duckdb
