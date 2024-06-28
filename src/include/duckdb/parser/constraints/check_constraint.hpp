//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::CHECK;

public:
	DUCKDB_API explicit CheckConstraint(unique_ptr<ParsedExpression> expression);

	unique_ptr<ParsedExpression> expression;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
