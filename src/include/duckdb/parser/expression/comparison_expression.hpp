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
	static constexpr const ExpressionClass TYPE = ExpressionClass::COMPARISON;

public:
	DUCKDB_API ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
	                                unique_ptr<ParsedExpression> right);

	unique_ptr<ParsedExpression> left;
	unique_ptr<ParsedExpression> right;

public:
	string ToString() const override;

	static bool Equal(const ComparisonExpression &a, const ComparisonExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return StringUtil::Format("(%s %s %s)", entry.left->ToString(),
		                          ExpressionTypeToOperator(entry.GetExpressionType()), entry.right->ToString());
	}

private:
	explicit ComparisonExpression(ExpressionType type);
};
} // namespace duckdb
