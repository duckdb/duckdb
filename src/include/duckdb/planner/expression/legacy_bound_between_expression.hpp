//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! This class only exists for legacy reasons (in particular supporting serialization / deserialization)
//! The bound between expression has been replaced by a function expression
class LegacyBoundBetweenExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LEGACY_BOUND_BETWEEN;

public:
	LegacyBoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
	                             unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive);

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

	static unique_ptr<Expression> DeserializeLegacyExpression(unique_ptr<Expression> input,
	                                                          unique_ptr<Expression> lower,
	                                                          unique_ptr<Expression> upper, bool lower_inclusive,
	                                                          bool upper_inclusive);

	string ToString() const override;
	unique_ptr<Expression> Copy() const override;

private:
	unique_ptr<Expression> input;
	unique_ptr<Expression> lower;
	unique_ptr<Expression> upper;
	bool lower_inclusive;
	bool upper_inclusive;
};
} // namespace duckdb
