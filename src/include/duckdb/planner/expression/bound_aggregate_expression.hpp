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
	BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
	                         unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info, bool distinct);

	//! The bound function expression
	AggregateFunction function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;
	//! True to aggregate on distinct values
	bool distinct;

	//! Filter for this aggregate
	unique_ptr<Expression> filter;

public:
	bool IsAggregate() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}
	bool PropagatesNullValues() const override;

	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;
	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
