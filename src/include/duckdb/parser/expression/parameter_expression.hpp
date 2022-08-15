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
class ParameterExpression : public ParsedExpression {
public:
	ParameterExpression();

	idx_t parameter_nr;

public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override;

	static bool Equals(const ParameterExpression *a, const ParameterExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
