//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_conjunction_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class BoundConjunctionExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CONJUNCTION;

public:
	explicit BoundConjunctionExpression(ExpressionType type);
	BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	vector<unique_ptr<Expression>> children;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	bool PropagatesNullValues() const override;

	unique_ptr<Expression> Copy() override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);

public:
	vector<ColumnBinding> getColumnBinding() override
	{
		vector<ColumnBinding> v;
		for(auto &child: children)
		{
			vector<ColumnBinding> v1 = child->getColumnBinding();
			v.insert(v1.begin(), v1.end(), v.end());
		}
		return v;
	}
};
} // namespace duckdb
