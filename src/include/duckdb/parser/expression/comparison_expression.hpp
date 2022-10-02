//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
//! ComparisonExpression represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has two children.
class ComparisonExpression : public ParsedExpression {
public:
	DUCKDB_API ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
	                                unique_ptr<ParsedExpression> right);

	unique_ptr<ParsedExpression> left;
	unique_ptr<ParsedExpression> right;

public:
	string ToString() const override;

	static bool Equals(const ComparisonExpression *a, const ComparisonExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return StringUtil::Format("(%s %s %s)", entry.left->ToString(), ExpressionTypeToOperator(entry.type),
		                          entry.right->ToString());
	}
};
} // namespace duckdb
