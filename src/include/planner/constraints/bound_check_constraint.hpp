//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/constraints/bound_check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "planner/bound_constraint.hpp"
#include "planner/expression.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class BoundCheckConstraint : public BoundConstraint {
public:
	BoundCheckConstraint(unique_ptr<Expression> expression)
	    : BoundConstraint(ConstraintType::CHECK), expression(move(expression)) {
	}

	unique_ptr<Expression> expression;
};

} // namespace duckdb
