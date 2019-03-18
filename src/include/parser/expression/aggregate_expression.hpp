//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/aggregate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
//! The AggregateExpression represents an aggregate in the query
class AggregateExpression : public ParsedExpression {
public:
	AggregateExpression(ExpressionType type, unique_ptr<ParsedExpression> child);

	//! The child of the aggregate expression
	unique_ptr<ParsedExpression> child;
public:
	bool IsAggregate() override {
		return true;
	}

	string GetName() const override;
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
