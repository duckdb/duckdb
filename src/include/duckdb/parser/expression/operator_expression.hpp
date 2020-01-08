//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/operator_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public ParsedExpression {
public:
	OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left = nullptr,
	                   unique_ptr<ParsedExpression> right = nullptr);

	vector<unique_ptr<ParsedExpression>> children;

public:
	string ToString() const override;

	static bool Equals(const OperatorExpression *a, const OperatorExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
