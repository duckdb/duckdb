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
};
} // namespace duckdb
