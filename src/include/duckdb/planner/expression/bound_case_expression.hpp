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

//! A reference to the operand of the enclosing simple CASE, used inside its WHEN comparisons.
class BoundCaseOperandExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CASE_OPERAND;

	explicit BoundCaseOperandExpression(LogicalType type)
	    : Expression(ExpressionType::CASE_OPERAND, TYPE, std::move(type)) {
	}
	bool IsFoldable() const override {
		return false;
	}
	bool IsScalar() const override {
		return false;
	}
	string ToString() const override {
		return "<case operand>";
	}
	unique_ptr<Expression> Copy() const override {
		auto result = make_uniq<BoundCaseOperandExpression>(return_type);
		result->CopyProperties(*this);
		return std::move(result);
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};

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

public:
	bool UseLegacySerialization() const {
		return operand != nullptr;
	}
	void LegacySerialize(Serializer &serializer) const;
	const unique_ptr<Expression> &Operand() const {
		return operand;
	}
	unique_ptr<Expression> &OperandMutable() {
		return operand;
	}
	const vector<BoundCaseCheck> &CaseChecks() const {
		return case_checks;
	}
	vector<BoundCaseCheck> &CaseChecksMutable() {
		return case_checks;
	}
	const Expression &Else() const {
		return *else_expr;
	}
	const unique_ptr<Expression> &ElseExpression() const {
		return else_expr;
	}
	unique_ptr<Expression> &ElseMutable() {
		return else_expr;
	}

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	unique_ptr<Expression> operand;
	vector<BoundCaseCheck> case_checks;
	unique_ptr<Expression> else_expr;
};
} // namespace duckdb
