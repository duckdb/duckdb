//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/constraints/not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"

namespace duckdb {

class NotNullConstraint : public Constraint {
public:
	NotNullConstraint(uint64_t index) : Constraint(ConstraintType::NOT_NULL), index(index){};
	virtual ~NotNullConstraint() {
	}

	//! Column index this constraint pertains to
	uint64_t index;

public:
	string ToString() const override {
		return "NOT NULL Constraint";
	}

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a NotNullConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
