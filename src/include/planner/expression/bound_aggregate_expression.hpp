//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_aggregate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"

namespace duckdb {
class AggregateFunctionCatalogEntry;

class BoundAggregateExpression : public Expression {
public:
	BoundAggregateExpression(TypeId return_type, unique_ptr<Expression> child,
	                         AggregateFunctionCatalogEntry *bound_aggregate, bool distinct);

	//! The bound function expression
	AggregateFunctionCatalogEntry *bound_aggregate;
	//! True to aggregate on distinct values
	bool distinct;
	//! The child of the aggregate expression
	unique_ptr<Expression> child;

public:
	bool IsAggregate() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	uint64_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
