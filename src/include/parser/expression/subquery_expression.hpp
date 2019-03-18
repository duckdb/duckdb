//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public ParsedExpression {
public:
	SubqueryExpression();

	//! The actual subquery
	unique_ptr<QueryNode> subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expression to compare with (in case of IN, ANY, ALL operators, empy for EXISTS queries and scalar
	//! subquery)
	unique_ptr<ParsedExpression> child;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators), empty otherwise
	ExpressionType comparison_type;
public:
	bool HasSubquery() override {
		return true;
	}
	bool IsScalar() override {
		return false;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression *other) const override;

	unique_ptr<ParsedExpression> Copy() override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);

	size_t ChildCount() const override;
	ParsedExpression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
	                  size_t index) override;
};
} // namespace duckdb
