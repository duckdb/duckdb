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
	static constexpr const ConstraintType TYPE = ConstraintType::NOT_NULL;

public:
	DUCKDB_API explicit NotNullConstraint(LogicalIndex index);
	DUCKDB_API ~NotNullConstraint() override;

	//! Column index this constraint pertains to
	LogicalIndex index;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
