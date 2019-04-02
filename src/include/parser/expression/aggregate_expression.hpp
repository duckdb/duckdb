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
	bool IsAggregate() const override {
		return true;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
