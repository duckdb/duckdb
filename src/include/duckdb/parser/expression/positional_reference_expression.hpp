//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/positional_reference_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class PositionalReferenceExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::POSITIONAL_REFERENCE;

public:
	DUCKDB_API explicit PositionalReferenceExpression(idx_t index);

public:
	idx_t Index() const {
		return index;
	}
	idx_t &IndexMutable() {
		return index;
	}
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	idx_t index;

private:
	PositionalReferenceExpression();
};
} // namespace duckdb
