//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
//! ComparisonExpression represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has two children.
class ComparisonExpression : public ParsedExpression {
public:
	ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right);

	static ExpressionType NegateComparisionExpression(ExpressionType type);
	static ExpressionType FlipComparisionExpression(ExpressionType type);

	unique_ptr<ParsedExpression> left;
	unique_ptr<ParsedExpression> right;
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
