//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_aggregate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include <memory>

namespace duckdb {

class BoundAggregateExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_AGGREGATE;

public:
	BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
	                         unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info,
	                         AggregateType aggr_type);

	//! The bound function expression
	AggregateFunction function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;
	//! The aggregate type (distinct or non-distinct)
	AggregateType aggr_type;

	//! Filter for this aggregate
	unique_ptr<Expression> filter;
	//! The order by expression for this aggregate - if any
	unique_ptr<BoundOrderModifier> order_bys;

public:
	bool IsDistinct() const {
		return aggr_type == AggregateType::DISTINCT;
	}

	bool IsAggregate() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}
	bool PropagatesNullValues() const override;

	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression &other) const override;
	unique_ptr<Expression> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
