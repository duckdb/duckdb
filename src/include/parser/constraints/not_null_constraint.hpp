//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/constraint/not_null_constraint.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class NotNullConstraint : public Constraint {
  public:
	NotNullConstraint(size_t index)
	    : Constraint(ConstraintType::NOT_NULL), index(index){};
	virtual ~NotNullConstraint() {
	}

	virtual std::unique_ptr<Constraint> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual std::string ToString() const {
		return "NOT NULL Constraint";
	}

	//! Serialize to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a NotNullConstraint
	static std::unique_ptr<Constraint> Deserialize(Deserializer &source);

	//! Column index this constraint pertains to
	size_t index;
};

} // namespace duckdb
