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
	DUCKDB_API explicit NotNullConstraint(column_t index);
	DUCKDB_API ~NotNullConstraint() override;

	//! Column index this constraint pertains to
	column_t index;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) override;
	//! Deserializes a NotNullConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &source);
};

} // namespace duckdb
