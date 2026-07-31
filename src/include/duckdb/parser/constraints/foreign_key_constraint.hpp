//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/foreign_key_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ForeignKeyConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::FOREIGN_KEY;

public:
	DUCKDB_API ForeignKeyConstraint(vector<Identifier> pk_columns, vector<Identifier> fk_columns, ForeignKeyInfo info);

	//! The set of main key table's columns
	vector<Identifier> pk_columns;
	//! The set of foreign key table's columns
	vector<Identifier> fk_columns;
	ForeignKeyInfo info;

public:
	DUCKDB_API string ToString() const override;
	//! Get the generated name of the foreign key constraint
	DUCKDB_API string GetName(const string& table_name) const;
	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);

private:
	ForeignKeyConstraint();
};

} // namespace duckdb
