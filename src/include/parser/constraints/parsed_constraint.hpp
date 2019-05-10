//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/constraints/parsed_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"

namespace duckdb {

//! The ParsedConstraint represents either a UNIQUE or PRIMARY KEY constraint
//! that is not fully resolved yet. When added to a table in the catalog, it
//! will be transformed to a proper PRIMARY KEY or UNIQUE constraint
class ParsedConstraint : public Constraint {
public:
	ParsedConstraint(ConstraintType type, uint64_t index)
	    : Constraint(ConstraintType::DUMMY), ctype(type), index(index) {
	}
	ParsedConstraint(ConstraintType type, vector<string> columns)
	    : Constraint(ConstraintType::DUMMY), ctype(type), index((uint64_t)-1), columns(columns) {
	}
	virtual ~ParsedConstraint() {
	}

	ConstraintType ctype;
	uint64_t index;
	vector<string> columns;

public:
	string ToString() const override {
		return "Dummy Constraint";
	}

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a ParsedConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
