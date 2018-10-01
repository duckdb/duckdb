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

namespace duckdb {

//! Constraint is the base class of any type of table constraint.
class Constraint : public Printable {
  public:
	Constraint(ConstraintType type) : type(type){};
	virtual ~Constraint() {}

	ConstraintType type;
};
} // namespace duckdb
