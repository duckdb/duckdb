//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

// Parameters come in three different types:
// auto-increment:
//	token: '?'
//	name: -
//	number: 0
// positional:
//	token: '$<number>'
//	name: -
//	number: <number>
// named:
//	token: '$<name>'
//	name: <name>
//	number: 0
enum class PreparedParamType : uint8_t { AUTO_INCREMENT, POSITIONAL, NAMED, INVALID };

class ParameterExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PARAMETER;

public:
	ParameterExpression();

	string identifier;

public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override;

	static bool Equal(const ParameterExpression &a, const ParameterExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
