//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/constraint/unique_constraint.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"

#include "storage/unique_index.hpp"

namespace duckdb {

class UniqueConstraint : public Constraint {
  public:
	UniqueConstraint(std::vector<TypeId> types, std::vector<size_t> keys)
	    : Constraint(ConstraintType::UNIQUE), index(types, keys, true){};

	UniqueConstraint(TypeId type, size_t key)
	    : Constraint(ConstraintType::UNIQUE), index({type}, {key}, true){};
	virtual ~UniqueConstraint() {}

	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	virtual std::string ToString() const { return "PRIMARY KEY Constraint"; }

	//! The index used to validate the constraint
	UniqueIndex index;
};

} // namespace duckdb
