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
#include "parser/parsed_expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
public:
	CheckConstraint(unique_ptr<ParsedExpression> expression)
	    : Constraint(ConstraintType::CHECK), expression(move(expression)){};
	virtual ~CheckConstraint() {
	}

	unique_ptr<ParsedExpression> expression;
public:
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
};

} // namespace duckdb
