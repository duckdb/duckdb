//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class BetweenExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BETWEEN;

public:
	DUCKDB_API BetweenExpression(unique_ptr<ParsedExpression> input, unique_ptr<ParsedExpression> lower,
	                             unique_ptr<ParsedExpression> upper);

	unique_ptr<ParsedExpression> input;
	unique_ptr<ParsedExpression> lower;
	unique_ptr<ParsedExpression> upper;

public:
	string ToString() const override;

	static bool Equal(const BetweenExpression &a, const BetweenExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	const ParsedExpression &Input() const {
		return *input;
	}
	const ParsedExpression &LowerBound() const {
		return *lower;
	}
	const ParsedExpression &UpperBound() const {
		return *upper;
	}

public:
	template <class T>
	static string ToString(const T &input, const T &lower, const T &upper) {
		return "(" + input.ToString() + " BETWEEN " + lower.ToString() + " AND " + upper.ToString() + ")";
	}

private:
	BetweenExpression();
};
} // namespace duckdb
