//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! CastExpression represents a type cast from one SQL type to another SQL type
class CastExpression : public ParsedExpression {
public:
	CastExpression(SQLType target, unique_ptr<ParsedExpression> child);

	//! The child of the cast expression
	unique_ptr<ParsedExpression> child;
	//! The type to cast to
	SQLType cast_type;

public:
	string ToString() const override;

	static bool Equals(const CastExpression *a, const CastExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
