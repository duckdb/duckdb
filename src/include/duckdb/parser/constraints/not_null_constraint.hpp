//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"

namespace duckdb {

class NotNullConstraint : public Constraint {
public:
	NotNullConstraint(column_t index) : Constraint(ConstraintType::NOT_NULL), index(index){};
	virtual ~NotNullConstraint() {
	}

	//! Column index this constraint pertains to
	column_t index;

public:
	string ToString() const override;

	unique_ptr<Constraint> Copy() override;

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a NotNullConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
