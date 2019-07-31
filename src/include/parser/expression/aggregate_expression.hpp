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
	AggregateExpression(string schema, string aggregate_name, bool distinct, unique_ptr<ParsedExpression> child);
	AggregateExpression(string aggregate_name, bool distinct, unique_ptr<ParsedExpression> child);

	//! Schema of the aggregate
	string schema;
	//! Aggregate name
	string aggregate_name;
	//! The child of the aggregate expression
	unique_ptr<ParsedExpression> child;
	//! Whether the aggregate is applied to distinct values
	bool distinct;

public:
	bool IsAggregate() const override {
		return true;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;
	uint64_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
