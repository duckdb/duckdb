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

class BoundBetweenExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_BETWEEN;

public:
	BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower, unique_ptr<Expression> upper,
	                       bool lower_inclusive, bool upper_inclusive);

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

public:
	ExpressionType LowerComparisonType() const {
		return lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_GREATERTHAN;
	}
	ExpressionType UpperComparisonType() const {
		return upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}

	const Expression &Input() const {
		return *input;
	}
	const Expression &LowerBound() const {
		return *lower;
	}
	const Expression &UpperBound() const {
		return *upper;
	}
	unique_ptr<Expression> &InputMutable() {
		return input;
	}
	unique_ptr<Expression> &LowerBoundMutable() {
		return lower;
	}
	unique_ptr<Expression> &UpperBoundMutable() {
		return upper;
	}
	bool LowerInclusive() const {
		return lower_inclusive;
	}
	bool UpperInclusive() const {
		return upper_inclusive;
	}

private:
	BoundBetweenExpression();

private:
	unique_ptr<Expression> input;
	unique_ptr<Expression> lower;
	unique_ptr<Expression> upper;
	bool lower_inclusive;
	bool upper_inclusive;
};
} // namespace duckdb
