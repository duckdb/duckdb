//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
class ParameterExpression : public ParsedExpression {
public:
	ParameterExpression();

	size_t parameter_nr;
public:
	bool IsScalar() override {
		return true;
	}
	bool HasParameter() override {
		return true;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression *other_) const override;

	unique_ptr<ParsedExpression> Copy() override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
