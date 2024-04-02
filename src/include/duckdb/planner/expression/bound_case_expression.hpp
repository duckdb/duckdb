//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct BoundCaseCheck {
	unique_ptr<Expression> when_expr;
	unique_ptr<Expression> then_expr;

	void Serialize(Serializer &serializer) const;
	static BoundCaseCheck Deserialize(Deserializer &deserializer);
};

class BoundCaseExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CASE;

public:
	explicit BoundCaseExpression(LogicalType type);
	BoundCaseExpression(unique_ptr<Expression> when_expr, unique_ptr<Expression> then_expr,
	                    unique_ptr<Expression> else_expr);

	vector<BoundCaseCheck> case_checks;
	unique_ptr<Expression> else_expr;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
