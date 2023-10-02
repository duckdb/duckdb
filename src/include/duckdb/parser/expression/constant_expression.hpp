//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! ConstantExpression represents a constant value in the query
class ConstantExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::CONSTANT;

public:
	DUCKDB_API explicit ConstantExpression(Value val);

	//! The constant value referenced
	Value value;

public:
	string ToString() const override;

	static bool Equal(const ConstantExpression &a, const ConstantExpression &b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	ConstantExpression();
};

} // namespace duckdb
