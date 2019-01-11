//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/constraints/not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class NotNullConstraint : public Constraint {
public:
	NotNullConstraint(size_t index) : Constraint(ConstraintType::NOT_NULL), index(index){};
	virtual ~NotNullConstraint() {
	}

	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	string ToString() const override {
		return "NOT NULL Constraint";
	}

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a NotNullConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);

	//! Column index this constraint pertains to
	size_t index;
};

} // namespace duckdb
