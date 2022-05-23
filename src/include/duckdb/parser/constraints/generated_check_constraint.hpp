//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! The GeneratedCheckConstraint is the same as a CheckConstraint but it's linked to a specific (generated) column
//! and it's updated/removed when the generated column is updated
class GeneratedCheckConstraint : public CheckConstraint {
public:
	DUCKDB_API explicit GeneratedCheckConstraint(column_t index, unique_ptr<ParsedExpression> expression);
	column_t column_index;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
