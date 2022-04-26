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

class GeneratedConstraint : public Constraint {
public:
	DUCKDB_API GeneratedConstraint(uint64_t index, unique_ptr<ParsedExpression> expression);

	//! The index of the column for which this constraint holds. Only used when the constraint relates to a single
	//! column, equal to DConstants::INVALID_INDEX if not used
	uint64_t index;
	unique_ptr<ParsedExpression>	expression;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	//! Deserializes a ParsedConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
