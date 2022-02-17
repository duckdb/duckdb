//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/conjunction_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a conjunction (AND/OR)
class ConjunctionExpression : public ParsedExpression {
public:
	DUCKDB_API explicit ConjunctionExpression(ExpressionType type);
	DUCKDB_API ConjunctionExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children);
	DUCKDB_API ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
	                                 unique_ptr<ParsedExpression> right);

	vector<unique_ptr<ParsedExpression>> children;

public:
	void AddExpression(unique_ptr<ParsedExpression> expr);

	string ToString() const override;

	static bool Equals(const ConjunctionExpression *a, const ConjunctionExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
