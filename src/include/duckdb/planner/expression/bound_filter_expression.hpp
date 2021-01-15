//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_filter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundFilterExpression : public Expression {
public:
	BoundFilterExpression(unique_ptr<Expression> filter);

	unique_ptr<Expression> filter;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
