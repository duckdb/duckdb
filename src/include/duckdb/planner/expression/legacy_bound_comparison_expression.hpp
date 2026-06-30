//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LegacyBoundComparisonExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LEGACY_BOUND_COMPARISON;

public:
	LegacyBoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	unique_ptr<Expression> left;
	unique_ptr<Expression> right;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

	static unique_ptr<Expression> DeserializeLegacyExpression(ExpressionType type, unique_ptr<Expression> left,
	                                                          unique_ptr<Expression> right);
};
} // namespace duckdb
