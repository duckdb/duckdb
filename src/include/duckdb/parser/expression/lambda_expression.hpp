//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/lambda_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! LambdaExpression represents either:
//!  1. A lambda operator that can be used for e.g. mapping an expression to a list
//!  2. An OperatorExpression with the "->" operator
//! Lambda expressions are written in the form of "params -> expr", e.g. "x -> x + 1"
class LambdaExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LAMBDA;

public:
	LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr);

	// we need the context to determine if this is a list of column references or an expression (for JSON)
	unique_ptr<ParsedExpression> lhs;

	vector<unique_ptr<ParsedExpression>> params;
	unique_ptr<ParsedExpression> expr;

public:
	string ToString() const override;

	static bool Equal(const LambdaExpression &a, const LambdaExpression &b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	LambdaExpression();
};

} // namespace duckdb
