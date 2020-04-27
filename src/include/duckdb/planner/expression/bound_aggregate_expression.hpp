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

namespace duckdb {
class BoundAggregateExpression : public Expression {
public:
	BoundAggregateExpression(TypeId return_type, AggregateFunction function, bool distinct);

	//! The bound function expression
	AggregateFunction function;
	//! True to aggregate on distinct values
	bool distinct;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! Argument types
	vector<SQLType> arguments;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;

public:
	bool IsAggregate() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
