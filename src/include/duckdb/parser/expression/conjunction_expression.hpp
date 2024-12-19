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

	vector<unique_ptr<ParsedExpression>> children;

public:
	void AddExpression(unique_ptr<ParsedExpression> expr);

	string ToString() const override;

	static bool Equal(const ConjunctionExpression &a, const ConjunctionExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		string result = "(" + entry.children[0]->ToString();
		for (idx_t i = 1; i < entry.children.size(); i++) {
			result += " " + ExpressionTypeToOperator(entry.GetExpressionType()) + " " + entry.children[i]->ToString();
		}
		return result + ")";
	}
};
} // namespace duckdb
