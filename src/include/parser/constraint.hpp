//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/constraint.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! Constraint is the base class of any type of table constraint.
class Constraint : public Printable {
  public:
	Constraint(ConstraintType type) : type(type){};
	virtual ~Constraint() {
	}

	virtual void Accept(SQLNodeVisitor *) {
	}

	ConstraintType type;

	//! Serializes a Constraint to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a Constraint, returns NULL if
	//! deserialization is not possible
	static std::unique_ptr<Constraint> Deserialize(Deserializer &source);
};
} // namespace duckdb
