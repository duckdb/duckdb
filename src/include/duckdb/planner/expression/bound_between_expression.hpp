//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_between_expression.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb
{

class BoundBetweenExpression : public Expression
{
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_BETWEEN;

public:
	BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower, unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive);

	unique_ptr<Expression> input;
	unique_ptr<Expression> lower;
	unique_ptr<Expression> upper;
	bool lower_inclusive;
	bool upper_inclusive;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);

public:
	ExpressionType LowerComparisonType() {
		return lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_GREATERTHAN;
	}
	ExpressionType UpperComparisonType() {
		return upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}

public:
	vector<ColumnBinding> getColumnBinding() override
	{
		vector<ColumnBinding> v;
		v = input->getColumnBinding();
		vector<ColumnBinding> v1 = lower->getColumnBinding();
		v.insert(v1.begin(), v1.end(), v.end());
		vector<ColumnBinding> v2 = upper->getColumnBinding();
		v.insert(v2.begin(), v2.end(), v.end());
		return v;
	}
};
} // namespace duckdb
