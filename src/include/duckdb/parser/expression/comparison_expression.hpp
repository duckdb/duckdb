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

public:
	const ParsedExpression &Left() const {
		return *left;
	}
	unique_ptr<ParsedExpression> &LeftMutable() {
		return left;
	}
	const ParsedExpression &Right() const {
		return *right;
	}
	unique_ptr<ParsedExpression> &RightMutable() {
		return right;
	}
	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T>
	static string ToString(ExpressionType type, const T &left, const T &right) {
		return StringUtil::Format("(%s %s %s)", left.ToString(), ExpressionTypeToOperator(type), right.ToString());
	}

private:
	unique_ptr<ParsedExpression> left;
	unique_ptr<ParsedExpression> right;

private:
	explicit ComparisonExpression(ExpressionType type);
	ComparisonExpression();
};
} // namespace duckdb
