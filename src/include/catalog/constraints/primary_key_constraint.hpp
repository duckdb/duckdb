//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/constraint/primary_key_constraint.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"

#include "storage/unique_index.hpp"

namespace duckdb {

class PrimaryKeyConstraint : public Constraint {
  public:
	PrimaryKeyConstraint(std::vector<TypeId> types, std::vector<size_t> keys)
	    : Constraint(ConstraintType::PRIMARY_KEY), index(types, keys, false){};
	virtual ~PrimaryKeyConstraint() {}

	virtual std::string ToString() const { return "PRIMARY KEY Constraint"; }

	//! The index used to validate the constraint
	UniqueIndex index;
};

} // namespace duckdb
