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

class BoundComparisonExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_COMPARISON;

public:
	BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	unique_ptr<Expression> left;
	unique_ptr<Expression> right;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

public:
	static LogicalType BindComparison(LogicalType left_type, LogicalType right_type);
};
} // namespace duckdb
