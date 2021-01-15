//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/filter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! The FilterExpression represents a Filter expression in the query
class FilterExpression : public ParsedExpression {
public:

	FilterExpression();

	unique_ptr<ParsedExpression> filter;

public:
	string ToString() const override;

	static bool Equals(const FilterExpression *a, const FilterExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
