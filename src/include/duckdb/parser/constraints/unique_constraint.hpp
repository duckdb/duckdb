//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class UniqueConstraint : public Constraint {
public:
	DUCKDB_API UniqueConstraint(LogicalIndex index, bool is_primary_key);
	DUCKDB_API UniqueConstraint(vector<string> columns, bool is_primary_key);

	//! The index of the column for which this constraint holds. Only used when the constraint relates to a single
	//! column, equal to DConstants::INVALID_INDEX if not used
	LogicalIndex index;
	//! The set of columns for which this constraint holds by name. Only used when the index field is not used.
	vector<string> columns;
	//! Whether or not this is a PRIMARY KEY constraint, or a UNIQUE constraint.
	bool is_primary_key;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	//! Deserializes a ParsedConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
