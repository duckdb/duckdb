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
	DUCKDB_API explicit NotNullConstraint(LogicalIndex index);
	DUCKDB_API ~NotNullConstraint() override;

	//! Column index this constraint pertains to
	LogicalIndex index;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	//! Deserializes a NotNullConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
