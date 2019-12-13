//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"

namespace duckdb {

class UniqueConstraint : public Constraint {
public:
	UniqueConstraint(uint64_t index, bool is_primary_key)
	    : Constraint(ConstraintType::UNIQUE), index(index), is_primary_key(is_primary_key) {
	}
	UniqueConstraint(vector<string> columns, bool is_primary_key)
	    : Constraint(ConstraintType::UNIQUE), index(INVALID_INDEX), columns(columns), is_primary_key(is_primary_key) {
	}

	//! The index of the column for which this constraint holds. Only used when the constraint relates to a single
	//! column, equal to INVALID_INDEX if not used
	uint64_t index;
	//! The set of columns for which this constraint holds by name. Only used when the index field is not used.
	vector<string> columns;
	//! Whether or not this is a PRIMARY KEY constraint, or a UNIQUE constraint.
	bool is_primary_key;

public:
	string ToString() const override;

	unique_ptr<Constraint> Copy() override;

	//! Serialize to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a ParsedConstraint
	static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
