//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/constraints/check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/string_util.hpp"
#include "parser/constraint.hpp"
#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
public:
	CheckConstraint(unique_ptr<Expression> expression)
	    : Constraint(ConstraintType::CHECK), expression(move(expression)){};
	virtual ~CheckConstraint() {
	}

	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	string ToString() const override {
		return "CHECK(" + expression->ToString() + ")";
	}

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a CheckConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);

	unique_ptr<Expression> expression;
};

} // namespace duckdb
