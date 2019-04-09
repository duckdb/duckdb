//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/constraints/bound_check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/constraints/check_constraint.hpp"
#include "planner/expression.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class BoundCheckConstraint : public Constraint {
public:
	BoundCheckConstraint(unique_ptr<Expression> expression, unique_ptr<CheckConstraint> unbound_constraint)
	    : Constraint(ConstraintType::CHECK), expression(move(expression)),
	      unbound_constraint(move(unbound_constraint)) {
	}

	unique_ptr<Expression> expression;
	unique_ptr<CheckConstraint> unbound_constraint;

public:
	string ToString() const override {
		return "CHECK(" + expression->ToString() + ")";
	}

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override {
		unbound_constraint->Serialize(serializer);
	}
};

} // namespace duckdb
