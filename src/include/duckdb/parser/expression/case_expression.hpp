//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! The CaseExpression represents a CASE expression in the query
class CaseExpression : public ParsedExpression {
public:
	// this expression has 3 children, (1) the check, (2) the result if the test is true, and (3) the result if the test
	// is false
	CaseExpression();

	unique_ptr<ParsedExpression> check;
	unique_ptr<ParsedExpression> result_if_true;
	unique_ptr<ParsedExpression> result_if_false;

public:
	string ToString() const override;

	static bool Equals(const CaseExpression *a, const CaseExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
