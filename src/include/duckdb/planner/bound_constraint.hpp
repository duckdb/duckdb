//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {
//! Bound equivalent of Constraint
class BoundConstraint {
public:
	explicit BoundConstraint(ConstraintType type) : type(type) {};
	virtual ~BoundConstraint() {
	}

	void Serialize(Serializer &serializer) const {
		serializer.Write(type);
	}

	static unique_ptr<BoundConstraint> Deserialize(Deserializer &source) {
		return make_uniq<BoundConstraint>(source.Read<ConstraintType>());
	}

	ConstraintType type;
};
} // namespace duckdb
