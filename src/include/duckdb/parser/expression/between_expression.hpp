//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! BetweenExpression represents a BETWEEN predicate
class BetweenExpression : public ParsedExpression {
public:
	BetweenExpression(unique_ptr<ParsedExpression> input, unique_ptr<ParsedExpression> lower, unique_ptr<ParsedExpression> upper, bool lower_inclusive, bool upper_inclusive);

	unique_ptr<ParsedExpression> input;
	unique_ptr<ParsedExpression> lower;
	unique_ptr<ParsedExpression> upper;
	bool lower_inclusive;
	bool upper_inclusive;

public:
	string ToString() const override;

	static bool Equals(const BetweenExpression *a, const BetweenExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};

} // namespace duckdb

