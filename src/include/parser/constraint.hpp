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
	virtual ~Constraint() {}

	virtual void Accept(SQLNodeVisitor *) {}

	ConstraintType type;

	//! Serializes a Constraint to a stand-alone binary blob
	std::unique_ptr<uint8_t[]> Serialize(size_t &size);
	//! Deserializes a blob back into a Constraint
	static std::unique_ptr<Constraint> Deserialize(uint8_t *dataptr,
	                                               size_t size);
};
} // namespace duckdb
