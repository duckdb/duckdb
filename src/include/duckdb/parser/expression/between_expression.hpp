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
	DUCKDB_API BetweenExpression(unique_ptr<ParsedExpression> input, unique_ptr<ParsedExpression> lower,
	                             unique_ptr<ParsedExpression> upper);

	unique_ptr<ParsedExpression> input;
	unique_ptr<ParsedExpression> lower;
	unique_ptr<ParsedExpression> upper;

public:
	string ToString() const override;

	static bool Equals(const BetweenExpression *a, const BetweenExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return entry.input->ToString() + " BETWEEN " + entry.lower->ToString() + " AND " + entry.upper->ToString();
	}
};
} // namespace duckdb
