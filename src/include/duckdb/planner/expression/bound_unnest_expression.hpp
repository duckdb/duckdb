//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_unnest_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Represents a function call that has been bound to a base function
class BoundUnnestExpression : public Expression {
public:
	BoundUnnestExpression(SQLType return_type);

	unique_ptr<Expression> child;
	//! The child and return type
	SQLType sql_return_type;

public:
	bool IsFoldable() const override;
	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
