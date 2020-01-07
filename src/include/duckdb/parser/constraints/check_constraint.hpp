//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
public:
	CheckConstraint(unique_ptr<ParsedExpression> expression)
	    : Constraint(ConstraintType::CHECK), expression(move(expression)){};

	unique_ptr<ParsedExpression> expression;

public:
	string ToString() const override;

	unique_ptr<Constraint> Copy() override;

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a CheckConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
