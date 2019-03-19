//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {
//! ConstantExpression represents a constant value in the query
class ConstantExpression : public ParsedExpression {
public:
	ConstantExpression(Value val);

	//! The constant value referenced
	Value value;
public:
	string ToString() const override;

	bool Equals(const BaseExpression *other_) const override;
	uint64_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
