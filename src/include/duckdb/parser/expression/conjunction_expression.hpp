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
	static constexpr const ExpressionClass TYPE = ExpressionClass::CONJUNCTION;

public:
	DUCKDB_API explicit ConjunctionExpression(ExpressionType type);
	DUCKDB_API ConjunctionExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children);
	DUCKDB_API ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
	                                 unique_ptr<ParsedExpression> right);

public:
	const vector<unique_ptr<ParsedExpression>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<ParsedExpression>> &GetChildrenMutable() {
		return children;
	}
	void AddExpression(unique_ptr<ParsedExpression> expr);

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	ConjunctionExpression();

private:
	vector<unique_ptr<ParsedExpression>> children;

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		auto &children = entry.GetChildren();
		string result = "(" + children[0]->ToString();
		for (idx_t i = 1; i < children.size(); i++) {
			result += " " + ExpressionTypeToOperator(entry.GetExpressionType()) + " " + children[i]->ToString();
		}
		return result + ")";
	}
};
} // namespace duckdb
