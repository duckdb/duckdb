//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundCastExpression : public Expression {
public:
	BoundCastExpression(TypeId target, unique_ptr<Expression> child, SQLType source_type, SQLType target_type);

	//! The child type
	unique_ptr<Expression> child;
	//! The SQL type of the child
	SQLType source_type;
	//! The SQL type to cast to
	SQLType target_type;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
