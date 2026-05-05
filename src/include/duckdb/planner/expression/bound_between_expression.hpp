//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

class BoundBetweenExpression : public BoundFunctionExpression {
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
		return *children[0];
	}
	const Expression &LowerBound() const {
		return *children[1];
	}
	const Expression &UpperBound() const {
		return *children[2];
	}
	const unique_ptr<Expression> &InputRef() const {
		return children[0];
	}
	const unique_ptr<Expression> &LowerBoundRef() const {
		return children[1];
	}
	const unique_ptr<Expression> &UpperBoundRef() const {
		return children[2];
	}
	unique_ptr<Expression> &InputMutable() {
		return children[0];
	}
	unique_ptr<Expression> &LowerBoundMutable() {
		return children[1];
	}
	unique_ptr<Expression> &UpperBoundMutable() {
		return children[2];
	}
	bool LowerInclusive() const {
		return lower_inclusive;
	}
	bool UpperInclusive() const {
		return upper_inclusive;
	}

private:
	bool lower_inclusive;
	bool upper_inclusive;
};
} // namespace duckdb
