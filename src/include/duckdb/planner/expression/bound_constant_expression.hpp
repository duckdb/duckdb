//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundConstantExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CONSTANT;

public:
	explicit BoundConstantExpression(Value value);

	Value value;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<Expression> FormatDeserialize(FormatDeserializer &deserializer);
};
} // namespace duckdb
