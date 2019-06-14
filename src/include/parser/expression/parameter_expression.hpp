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

	index_t parameter_nr;

public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;
	uint64_t Hash() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
